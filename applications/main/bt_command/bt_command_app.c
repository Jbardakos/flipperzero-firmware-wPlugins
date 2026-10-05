#include "bt_command_app.h"

#include <furi.h>
#include <furi_hal_bt.h>
#include <bt/bt_service/bt.h>
#include <gui/gui.h>
#include <gui/elements.h>
#include <input/input.h>

#define TAG "BtCommand"

typedef struct {
    Gui* gui;
    ViewPort* view_port;
    FuriMessageQueue* event_queue;
    Bt* bt;
} BtCommandApp;

static void bt_command_app_render_callback(Canvas* canvas, void* context) {
    UNUSED(context);

    canvas_clear(canvas);
    canvas_set_font(canvas, FontPrimary);
    canvas_draw_str(canvas, 2, 12, "BT Command");

    canvas_set_font(canvas, FontSecondary);
    canvas_draw_str(canvas, 2, 26, "channel: BLE serial RPC");
    canvas_draw_str(canvas, 2, 38, "cmds: 101..105");
    canvas_draw_str(canvas, 2, 50, "flow: 4-byte credit");
    canvas_draw_str(canvas, 2, 62, "Back: exit");
}

static void bt_command_app_input_callback(InputEvent* event, void* context) {
    furi_assert(event);
    furi_assert(context);

    BtCommandApp* app = context;
    furi_message_queue_put(app->event_queue, event, FuriWaitForever);
}

static BtCommandApp* bt_command_app_alloc(void) {
    BtCommandApp* app = malloc(sizeof(BtCommandApp));

    // BT record: the BT service is what actually feeds the BLE RPC session.
    app->bt = furi_record_open(RECORD_BT);

    app->event_queue = furi_message_queue_alloc(8, sizeof(InputEvent));

    app->view_port = view_port_alloc();
    view_port_draw_callback_set(app->view_port, bt_command_app_render_callback, app);
    view_port_input_callback_set(app->view_port, bt_command_app_input_callback, app);

    app->gui = furi_record_open(RECORD_GUI);
    gui_add_view_port(app->gui, app->view_port, GuiLayerFullscreen);

    return app;
}

static void bt_command_app_free(BtCommandApp* app) {
    furi_assert(app);

    gui_remove_view_port(app->gui, app->view_port);
    view_port_free(app->view_port);
    furi_record_close(RECORD_GUI);

    furi_message_queue_free(app->event_queue);

    furi_record_close(RECORD_BT);
    free(app);
}

int32_t bt_command_app(void* p) {
    UNUSED(p);

    BtCommandApp* app = bt_command_app_alloc();

    BtStatus last_status = (BtStatus)-1;
    FURI_LOG_I(TAG, "BT Command console started");

    InputEvent event;
    while(true) {
        if(furi_message_queue_get(app->event_queue, &event, 250) == FuriStatusOk) {
            if(event.type == InputTypeShort && event.key == InputKeyBack) {
                break;
            }
        }

        // Report channel transitions on the log (the on-screen text is static).
        BtStatus status = bt_get_status(app->bt);
        if(status != last_status) {
            FURI_LOG_I(TAG, "BT status: %d", (int)status);
            last_status = status;
        }
    }

    FURI_LOG_I(TAG, "BT Command console stopped");
    bt_command_app_free(app);

    return 0;
}
