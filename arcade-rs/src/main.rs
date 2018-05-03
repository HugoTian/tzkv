extern crate sdl2;
mod events; // will search for events.rs or events/mod.rs
use sdl2::pixels::Color;
use std::thread;
use std::time::Duration;
use events::Events;

fn main() {
    // Initialize SDL2
    let sdl_context = sdl2::init().unwrap();
    let video = sdl_context.video().unwrap();

    // Create the window
    let window = video.window("ArcadeRS Shooter", 800, 600)
        .position_centered().opengl()
        .build().unwrap();

    let mut renderer = window.renderer()
        .accelerated()
        .build().unwrap();

    // prepare the events record
    let mut events = Events::new(sdl_context.event_pump().unwrap());

    loop {
        events.pump();

        if events.quit || events.key_escape {
            break;
        }

        // Render a fully black window
        renderer.set_draw_color(Color::RGB(0, 0, 0));
        renderer.clear();
        renderer.present();
    }
    thread::sleep(Duration::from_millis(3000));
}