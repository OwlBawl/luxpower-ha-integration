from typing import Final
from homeassistant.const import Platform

DOMAIN = "lxp_modbus"

PLATFORMS: Final = [
    Platform.SENSOR,
    Platform.NUMBER,
    Platform.TIME,
    Platform.SELECT,
    Platform.BUTTON,
    Platform.SWITCH,
]

# Protocol configuration
CONF_PROTOCOL = "protocol"
PROTOCOL_TCP = "tcp"
PROTOCOL_RTU = "rtu"

# TCP configuration
CONF_HOST = "host"
CONF_PORT = "port"

# RTU configuration
CONF_SERIAL_PORT = "serial_port"
CONF_BAUDRATE = "baudrate"
CONF_PARITY = "parity"
CONF_STOPBITS = "stopbits"
CONF_BYTESIZE = "bytesize"
CONF_SLAVE_ID = "slave_id"

# Common configuration
CONF_DONGLE_SERIAL = "dongle_serial"
CONF_INVERTER_SERIAL = "inverter_serial"
CONF_POLL_INTERVAL = "poll_interval"
CONF_ENTITY_PREFIX = "entity_prefix"
CONF_RATED_POWER = "rated_power"
CONF_READ_ONLY = "read_only"
CONF_REGISTER_BLOCK_SIZE = "register_block_size"
CONF_CONNECTION_RETRIES = "connection_retries"
CONF_ENABLE_DEVICE_GROUPING = "enable_device_grouping"

INTEGRATION_TITLE = "Luxpower Modbus-to-USB"


DEFAULT_POLL_INTERVAL = 60  # or whatever default you prefer, in seconds
DEFAULT_ENTITY_PREFIX = ""
DEFAULT_RATED_POWER = 5000
DEFAULT_READ_ONLY = False
DEFAULT_PORT = 8000
DEFAULT_REGISTER_BLOCK_SIZE = 125
DEFAULT_CONNECTION_RETRIES = 3
DEFAULT_ENABLE_DEVICE_GROUPING = True

# RTU defaults - Based on LuxPower Modbus RTU Protocol specification
DEFAULT_BAUDRATE = 19200  # Per protocol document: 19200bps
DEFAULT_PARITY = "N"      # Per protocol document: no parity bits
DEFAULT_STOPBITS = 1      # Per protocol document: one stop bit
DEFAULT_BYTESIZE = 8      # Per protocol document: 8 data bits
DEFAULT_SLAVE_ID = 1      # Standard default slave ID

# Available options for RTU configuration
BAUDRATE_OPTIONS = [9600, 19200, 38400, 57600, 115200]
PARITY_OPTIONS = ["N", "E", "O"]  # None, Even, Odd
STOPBITS_OPTIONS = [1, 2]
BYTESIZE_OPTIONS = [7, 8]

# RTU Protocol specifications
RTU_MIN_POLL_INTERVAL = 1  # Minimum polling period: 1s per protocol document

# Legacy firmware may only support smaller block sizes
LEGACY_REGISTER_BLOCK_SIZE = 40
TOTAL_REGISTERS = 300 # Total number of registers available

# Packet recovery constants
MAX_PACKET_RECOVERY_ATTEMPTS = 3
MAX_PACKET_SIZE = 1024  # Maximum reasonable packet size in bytes
PACKET_RECOVERY_TIMEOUT = 2  # Timeout for packet recovery operations

RESPONSE_OVERHEAD: Final = 37 # minimum resposne length received from inverter (technical information)
WRITE_RESPONSE_LENGTH = 76 # Based on documentation for a single write ack