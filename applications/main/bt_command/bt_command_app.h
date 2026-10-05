#pragma once

/**
 * BT Command app.
 *
 * Status console for the BLE command channel used by a Hermes agent.
 *
 * Where the pieces live:
 *  - Transport (BLE serial service, GATT characteristics and the 4-byte credit
 *    flow control) is the firmware's own BT service:
 *    applications/services/bt/bt_service/bt.c +
 *    targets/f7/ble_glue/services/serial_service.c
 *  - Command decode/dispatch for tags 101..105 is in
 *    applications/services/rpc/rpc.c
 *  - The wire schema is assets/protobuf/flipper.proto
 *
 * This app deliberately does NOT start its own BLE profile or install a serial
 * event callback: the RPC session is fed by the BT service, and taking the
 * callback over would starve that session. It only reports channel state.
 */

#include <furi.h>
