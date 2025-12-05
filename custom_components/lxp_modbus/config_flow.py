import asyncio
import logging
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import callback

from .const import *
from .classes.lxp_request_builder import LxpRequestBuilder
from .classes.lxp_response import LxpResponse
from .utils import decode_model_from_registers

_LOGGER = logging.getLogger(__name__)

def get_serial_ports():
    """Get list of available serial ports."""
    try:
        import serial.tools.list_ports
        ports = serial.tools.list_ports.comports()
        return [port.device for port in ports]
    except Exception as e:
        _LOGGER.warning(f"Could not enumerate serial ports: {e}")
        return []

def validate_serial(value):
    """Validate that the serial number is exactly 10 characters."""
    value = str(value)
    if len(value) != 10:
        raise vol.Invalid("Serial number must be exactly 10 characters.")
    return value

def validate_connection_retries(value):
    """Validate that the connection retries value is between 1 and 10."""
    value = int(value)
    if value < 1 or value > 10:
        raise vol.Invalid("Connection retry attempts must be between 1 and 10.")
    return value

async def get_inverter_model_from_device_tcp(host, port, dongle_serial, inverter_serial):
    """Attempt to connect to the inverter via TCP and read the model."""
    try:
        reader, writer = await asyncio.open_connection(host, port)
        req = LxpRequestBuilder.prepare_packet_for_read(dongle_serial.encode(), inverter_serial.encode(), 7, 2, 3)
        writer.write(req)
        await writer.drain()
        response_buf = await reader.read(512)
        writer.close()
        await writer.wait_closed()
        if not response_buf: return None
        response = LxpResponse(response_buf)
        if response.packet_error: return None
        model = decode_model_from_registers(response.parsed_values_dictionary)
        return model
    except Exception:
        return None

async def get_inverter_model_from_device_rtu(serial_port, baudrate, parity, stopbits, bytesize, slave_id):
    """Attempt to connect to the inverter via RTU and read the model."""
    try:
        from pymodbus.client import ModbusSerialClient
        
        client = ModbusSerialClient(
            port=serial_port,
            baudrate=baudrate,
            parity=parity,
            stopbits=stopbits,
            bytesize=bytesize,
            timeout=3
        )
        
        if not client.connect():
            return None
        
        # Read registers 7-8 (model information) using function code 3
        result = client.read_holding_registers(address=7, count=2, slave=slave_id)
        client.close()
        
        if result.isError():
            return None
        
        # Convert result to dictionary format expected by decode_model_from_registers
        registers_dict = {7 + i: result.registers[i] for i in range(len(result.registers))}
        model = decode_model_from_registers(registers_dict)
        return model
    except Exception as e:
        _LOGGER.warning(f"Failed to get model from RTU device: {e}")
        return None

class LxpModbusConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle the initial setup flow for the component."""
    VERSION = 1

    def __init__(self):
        """Initialize the config flow."""
        self._protocol = None
        self._config = {}

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: config_entries.ConfigEntry):
        """Get the options flow for this handler."""
        return LxpModbusOptionsFlow()

    async def async_step_user(self, user_input=None):
        """Handle protocol selection."""
        if user_input is not None:
            self._protocol = user_input[CONF_PROTOCOL]
            if self._protocol == PROTOCOL_TCP:
                return await self.async_step_tcp()
            else:
                return await self.async_step_rtu()
        
        data_schema = vol.Schema({
            vol.Required(CONF_PROTOCOL, default=PROTOCOL_TCP): vol.In({
                PROTOCOL_TCP: "Modbus TCP (WiFi Dongle)",
                PROTOCOL_RTU: "Modbus RTU (RS-485/USB)"
            })
        })
        
        return self.async_show_form(
            step_id="user",
            data_schema=data_schema,
            description_placeholders={
                "protocol_info": "Select the communication protocol for your LuxPower inverter"
            }
        )

    async def async_step_tcp(self, user_input=None):
        """Handle TCP configuration."""
        errors = {}
        if user_input is not None:
            try:
                validate_serial(user_input[CONF_DONGLE_SERIAL])
                validate_serial(user_input[CONF_INVERTER_SERIAL])
                
                # Validate connection retries
                try:
                    validate_connection_retries(user_input.get(CONF_CONNECTION_RETRIES, DEFAULT_CONNECTION_RETRIES))
                except vol.Invalid:
                    errors[CONF_CONNECTION_RETRIES] = "invalid_connection_retries"
                
                if not errors:
                    model = await get_inverter_model_from_device_tcp(
                        user_input[CONF_HOST], 
                        user_input[CONF_PORT], 
                        user_input[CONF_DONGLE_SERIAL], 
                        user_input[CONF_INVERTER_SERIAL]
                    )
                    if not model:
                        errors["base"] = "model_fetch_failed"
                    else:
                        user_input[CONF_PROTOCOL] = PROTOCOL_TCP
                        user_input["model"] = model
                        title = user_input.get(CONF_ENTITY_PREFIX) or "Luxpower Inverter"
                        return self.async_create_entry(title=title, data=user_input)
            except vol.Invalid:
                errors["base"] = "invalid_serial"
        
        data_schema = vol.Schema({
            vol.Required(CONF_HOST): str,
            vol.Required(CONF_PORT, default=DEFAULT_PORT): int,
            vol.Required(CONF_DONGLE_SERIAL): str,
            vol.Required(CONF_INVERTER_SERIAL): str,
            vol.Required(CONF_POLL_INTERVAL, default=DEFAULT_POLL_INTERVAL): vol.All(int, vol.Range(min=2, max=600)),
            vol.Optional(CONF_ENTITY_PREFIX, default=DEFAULT_ENTITY_PREFIX): str,
            vol.Required(CONF_RATED_POWER, default=DEFAULT_RATED_POWER): vol.All(int, vol.Range(min=1000, max=100000)),
            vol.Optional(CONF_READ_ONLY, default=DEFAULT_READ_ONLY): bool,
            vol.Optional(CONF_REGISTER_BLOCK_SIZE, default=DEFAULT_REGISTER_BLOCK_SIZE): vol.In([DEFAULT_REGISTER_BLOCK_SIZE, LEGACY_REGISTER_BLOCK_SIZE]),
            vol.Required(CONF_CONNECTION_RETRIES, default=DEFAULT_CONNECTION_RETRIES): vol.All(int, vol.Range(min=1, max=10)),
            vol.Optional(CONF_ENABLE_DEVICE_GROUPING, default=DEFAULT_ENABLE_DEVICE_GROUPING): bool,
        })
        return self.async_show_form(
            step_id="tcp", 
            data_schema=self.add_suggested_values_to_schema(data_schema, user_input), 
            errors=errors
        )

    async def async_step_rtu(self, user_input=None):
        """Handle RTU configuration."""
        errors = {}
        if user_input is not None:
            try:
                # Validate connection retries
                try:
                    validate_connection_retries(user_input.get(CONF_CONNECTION_RETRIES, DEFAULT_CONNECTION_RETRIES))
                except vol.Invalid:
                    errors[CONF_CONNECTION_RETRIES] = "invalid_connection_retries"
                
                if not errors:
                    model = await get_inverter_model_from_device_rtu(
                        user_input[CONF_SERIAL_PORT],
                        user_input[CONF_BAUDRATE],
                        user_input[CONF_PARITY],
                        user_input[CONF_STOPBITS],
                        user_input[CONF_BYTESIZE],
                        user_input[CONF_SLAVE_ID]
                    )
                    if not model:
                        errors["base"] = "model_fetch_failed_rtu"
                    else:
                        user_input[CONF_PROTOCOL] = PROTOCOL_RTU
                        user_input["model"] = model
                        # RTU doesn't use dongle/inverter serials in the same way
                        # Set placeholder values to maintain compatibility
                        user_input[CONF_DONGLE_SERIAL] = "RTU_DEVICE"
                        user_input[CONF_INVERTER_SERIAL] = f"RTU_{user_input[CONF_SLAVE_ID]:010d}"
                        title = user_input.get(CONF_ENTITY_PREFIX) or "Luxpower Inverter RTU"
                        return self.async_create_entry(title=title, data=user_input)
            except Exception as e:
                _LOGGER.error(f"RTU configuration error: {e}")
                errors["base"] = "unknown"
        
        # Get available serial ports
        serial_ports = get_serial_ports()
        if not serial_ports:
            serial_ports = ["/dev/ttyUSB0", "/dev/ttyUSB1", "/dev/ttyAMA0", "COM1", "COM2", "COM3"]
        
        data_schema = vol.Schema({
            vol.Required(CONF_SERIAL_PORT): vol.In(serial_ports),
            vol.Required(CONF_BAUDRATE, default=DEFAULT_BAUDRATE): vol.In(BAUDRATE_OPTIONS),
            vol.Required(CONF_PARITY, default=DEFAULT_PARITY): vol.In(PARITY_OPTIONS),
            vol.Required(CONF_STOPBITS, default=DEFAULT_STOPBITS): vol.In(STOPBITS_OPTIONS),
            vol.Required(CONF_BYTESIZE, default=DEFAULT_BYTESIZE): vol.In(BYTESIZE_OPTIONS),
            vol.Required(CONF_SLAVE_ID, default=DEFAULT_SLAVE_ID): vol.All(int, vol.Range(min=1, max=247)),
            vol.Required(CONF_POLL_INTERVAL, default=max(DEFAULT_POLL_INTERVAL, RTU_MIN_POLL_INTERVAL)): vol.All(int, vol.Range(min=RTU_MIN_POLL_INTERVAL, max=600)),
            vol.Optional(CONF_ENTITY_PREFIX, default=DEFAULT_ENTITY_PREFIX): str,
            vol.Required(CONF_RATED_POWER, default=DEFAULT_RATED_POWER): vol.All(int, vol.Range(min=1000, max=100000)),
            vol.Optional(CONF_READ_ONLY, default=DEFAULT_READ_ONLY): bool,
            vol.Optional(CONF_REGISTER_BLOCK_SIZE, default=DEFAULT_REGISTER_BLOCK_SIZE): vol.In([DEFAULT_REGISTER_BLOCK_SIZE, LEGACY_REGISTER_BLOCK_SIZE]),
            vol.Required(CONF_CONNECTION_RETRIES, default=DEFAULT_CONNECTION_RETRIES): vol.All(int, vol.Range(min=1, max=10)),
            vol.Optional(CONF_ENABLE_DEVICE_GROUPING, default=DEFAULT_ENABLE_DEVICE_GROUPING): bool,
        })
        return self.async_show_form(
            step_id="rtu", 
            data_schema=self.add_suggested_values_to_schema(data_schema, user_input), 
            errors=errors
        )
class LxpModbusOptionsFlow(config_entries.OptionsFlow):
    """Handle an options flow (reconfiguration) for the component."""

    async def async_step_init(self, user_input=None):
        """Manage the options."""
        errors = {}
        current_config = {**self.config_entry.data, **self.config_entry.options}
        protocol = current_config.get(CONF_PROTOCOL, PROTOCOL_TCP)

        if user_input is not None:
            try:
                # Validate connection retries
                try:
                    validate_connection_retries(user_input.get(CONF_CONNECTION_RETRIES, DEFAULT_CONNECTION_RETRIES))
                except vol.Invalid:
                    errors[CONF_CONNECTION_RETRIES] = "invalid_connection_retries"
                
                if not errors:
                    # Validate based on protocol
                    if protocol == PROTOCOL_TCP:
                        validate_serial(user_input[CONF_DONGLE_SERIAL])
                        validate_serial(user_input[CONF_INVERTER_SERIAL])
                        model = await get_inverter_model_from_device_tcp(
                            user_input[CONF_HOST],
                            user_input[CONF_PORT],
                            user_input[CONF_DONGLE_SERIAL],
                            user_input[CONF_INVERTER_SERIAL]
                        )
                        if not model:
                            errors["base"] = "model_fetch_failed"
                    else:  # RTU
                        model = await get_inverter_model_from_device_rtu(
                            user_input[CONF_SERIAL_PORT],
                            user_input[CONF_BAUDRATE],
                            user_input[CONF_PARITY],
                            user_input[CONF_STOPBITS],
                            user_input[CONF_BYTESIZE],
                            user_input[CONF_SLAVE_ID]
                        )
                        if not model:
                            errors["base"] = "model_fetch_failed_rtu"
                    
                    if not errors:
                        new_data = {**current_config, **user_input}
                        new_data["model"] = model
                        
                        self.hass.config_entries.async_update_entry(
                            self.config_entry, data=new_data, options={}
                        )
                        await self.hass.config_entries.async_reload(self.config_entry.entry_id)
                        return self.async_create_entry(title="", data={})

            except vol.Invalid:
                errors["base"] = "invalid_serial"
            except Exception as e:
                _LOGGER.error(f"Options flow error: {e}")
                errors["base"] = "unknown"
        
        # Build schema based on protocol
        if protocol == PROTOCOL_TCP:
            options_schema = vol.Schema({
                vol.Required(CONF_HOST, default=current_config.get(CONF_HOST)): str,
                vol.Required(CONF_PORT, default=current_config.get(CONF_PORT)): int,
                vol.Required(CONF_DONGLE_SERIAL, default=current_config.get(CONF_DONGLE_SERIAL)): str,
                vol.Required(CONF_INVERTER_SERIAL, default=current_config.get(CONF_INVERTER_SERIAL)): str,
                vol.Required(CONF_POLL_INTERVAL, default=current_config.get(CONF_POLL_INTERVAL)): vol.All(int, vol.Range(min=2, max=600)),
                vol.Optional(CONF_ENTITY_PREFIX, default=current_config.get(CONF_ENTITY_PREFIX, '')): vol.All(str),
                vol.Required(CONF_RATED_POWER, default=current_config.get(CONF_RATED_POWER)): vol.All(int, vol.Range(min=1000, max=100000)),
                vol.Optional(CONF_READ_ONLY, default=current_config.get(CONF_READ_ONLY, DEFAULT_READ_ONLY)): bool,
                vol.Optional(CONF_REGISTER_BLOCK_SIZE, default=current_config.get(CONF_REGISTER_BLOCK_SIZE, DEFAULT_REGISTER_BLOCK_SIZE)): vol.In([DEFAULT_REGISTER_BLOCK_SIZE, LEGACY_REGISTER_BLOCK_SIZE]),
                vol.Required(CONF_CONNECTION_RETRIES, default=current_config.get(CONF_CONNECTION_RETRIES, DEFAULT_CONNECTION_RETRIES)): vol.All(int, vol.Range(min=1, max=10)),
                vol.Optional(CONF_ENABLE_DEVICE_GROUPING, default=current_config.get(CONF_ENABLE_DEVICE_GROUPING, DEFAULT_ENABLE_DEVICE_GROUPING)): bool,
            })
        else:  # RTU
            serial_ports = get_serial_ports()
            if not serial_ports:
                serial_ports = ["/dev/ttyUSB0", "/dev/ttyUSB1", "/dev/ttyAMA0", "COM1", "COM2", "COM3"]
            
            options_schema = vol.Schema({
                vol.Required(CONF_SERIAL_PORT, default=current_config.get(CONF_SERIAL_PORT)): vol.In(serial_ports),
                vol.Required(CONF_BAUDRATE, default=current_config.get(CONF_BAUDRATE, DEFAULT_BAUDRATE)): vol.In(BAUDRATE_OPTIONS),
                vol.Required(CONF_PARITY, default=current_config.get(CONF_PARITY, DEFAULT_PARITY)): vol.In(PARITY_OPTIONS),
                vol.Required(CONF_STOPBITS, default=current_config.get(CONF_STOPBITS, DEFAULT_STOPBITS)): vol.In(STOPBITS_OPTIONS),
                vol.Required(CONF_BYTESIZE, default=current_config.get(CONF_BYTESIZE, DEFAULT_BYTESIZE)): vol.In(BYTESIZE_OPTIONS),
                vol.Required(CONF_SLAVE_ID, default=current_config.get(CONF_SLAVE_ID, DEFAULT_SLAVE_ID)): vol.All(int, vol.Range(min=1, max=247)),
                vol.Required(CONF_POLL_INTERVAL, default=current_config.get(CONF_POLL_INTERVAL)): vol.All(int, vol.Range(min=2, max=600)),
                vol.Optional(CONF_ENTITY_PREFIX, default=current_config.get(CONF_ENTITY_PREFIX, '')): vol.All(str),
                vol.Required(CONF_RATED_POWER, default=current_config.get(CONF_RATED_POWER)): vol.All(int, vol.Range(min=1000, max=100000)),
                vol.Optional(CONF_READ_ONLY, default=current_config.get(CONF_READ_ONLY, DEFAULT_READ_ONLY)): bool,
                vol.Optional(CONF_REGISTER_BLOCK_SIZE, default=current_config.get(CONF_REGISTER_BLOCK_SIZE, DEFAULT_REGISTER_BLOCK_SIZE)): vol.In([DEFAULT_REGISTER_BLOCK_SIZE, LEGACY_REGISTER_BLOCK_SIZE]),
                vol.Required(CONF_CONNECTION_RETRIES, default=current_config.get(CONF_CONNECTION_RETRIES, DEFAULT_CONNECTION_RETRIES)): vol.All(int, vol.Range(min=1, max=10)),
                vol.Optional(CONF_ENABLE_DEVICE_GROUPING, default=current_config.get(CONF_ENABLE_DEVICE_GROUPING, DEFAULT_ENABLE_DEVICE_GROUPING)): bool,
            })

        return self.async_show_form(
            step_id="init",
            data_schema=self.add_suggested_values_to_schema(options_schema, current_config),
            errors=errors,
        )