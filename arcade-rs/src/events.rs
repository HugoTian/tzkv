use sdl2::EventPump;  // since we extern in main.rs

pub struct Events {
    pump : EventPump,
    pub quit: bool,
    pub key_escape: bool,
}

impl Events {
    pub fn new(pump: EventsPump) -> Events{
        Events {
            pump: pump,

            quit: false,
            key_escape: false
        } // The value of the function is set to that of its last non-semicolon-terminated expression.
    }

    // update the events record
    pub fn pump(&mut self) {
        for event in self.pump.poll_iter() {
            use sdl2::event::Event::*;
            use sdl2::keyboard::Keycode::*;

            match event {
                Quit { .. } => self.quit = true,

                KeyDown { keyCode, .. } => match keycode {
                    Some(Escape) => self.key_escape = true,
                    _ => {}
                },

                KeyUp {keyCode, .. } => match keyCode {
                    Some(Escape) => self.key_escape = false,
                    _ => {}
                } ,

                _ => {}
            };
        }
    }
}