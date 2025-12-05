# Protocol Comparison: Modbus TCP vs Modbus RTU

This document explains the differences between the two communication protocols supported by this integration.

## Overview

LuxPower inverters support two different Modbus protocols depending on the connection type:

| Protocol | Connection Type | Use Case |
|----------|----------------|----------|
| **Custom TCP Protocol** | WiFi Dongle (Ethernet/WiFi) | Wireless connection via network |
| **Standard Modbus RTU** | Direct RS-485 (Serial/USB) | Wired connection via RS-485 converter |

## Important: Different Protocols!

These are **NOT the same protocol**. The TCP implementation uses a **proprietary LuxPower packet format**, while RTU uses **standard Modbus RTU**.

## Detailed Comparison

### Custom TCP Protocol (WiFi Dongle)

**Packet Structure:**