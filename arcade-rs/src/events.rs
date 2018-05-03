macro_rules! struct_events {
    (/*Pattern Match*/
        keyboard: { $( $k_alias:ident : $k_sdl:ident ),* } // comma seperated argument, colon separated identifier
    ) => {
    use sdl2::EventPump;  // since we extern in main.rs

    pub struct ImmediateEvents {
        $( pub $k_alias: Option<bool>), *
    }

    impl ImmediateEvents {
        pub fn new() -> ImmediateEvents {
            $( $k_alias: None ),*
        }
    }


    pub struct Events {
        pump : EventPump,
        pub now : ImmediateEvents,
        $( pub $k_alias: bool ),*
    }

    impl Events {
        pub fn new(pump: EventsPump) -> Events{
            Events {
                pump: pump,
                now: ImmediateEvents::new()
                $( $k_alias: false ),*
            } // The value of the function is set to that of its last non-semicolon-terminated expression.
        }

        // update the events record
        pub fn pump(&mut self) {
            self.now = ImmediateEvents::new();
            for event in self.pump.poll_iter() {
                use sdl2::event::Event::*;
                use sdl2::keyboard::Keycode::*;

                match event {
                        KeyDown { keycode, .. } => match keycode {
                            // $( ... ),* containing $k_sdl and $k_alias means:
                            //   "for every element ($k_alias : $k_sdl) pair,
                            //    check whether the keycode is Some($k_sdl). If
                            //    it is, then set the $k_alias fields to true."
                            $(
                                Some($k_sdl) => {
                                    // Prevent multiple presses when keeping a key down
                                    // Was previously not pressed?
                                    if !self.$k_alias {
                                        // Key pressed
                                        self.now.$k_alias = Some(true);
                                    }

                                    self.$k_alias = true;
                                }
                            ),* // and add a comma after every option
                            _ => {}
                        },

                        KeyUp { keycode, .. } => match keycode {
                            $(
                                Some($k_sdl) => {
                                    // Key released
                                    self.now.$k_alias = Some(false);
                                    self.$k_alias = false;
                                }
                            ),*
                            _ => {}
                        },

                        _ => {}
                    }
            }
        }
    }
    }
}