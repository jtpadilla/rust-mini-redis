use std::sync;
use std::sync::{Arc, Mutex};
use std::thread;

fn main() {
    const N: usize = 10;

    // Se utiliza Arc para compartir memoria entre varios threads y los
    // datos compartido (que estan dentro del Arc) se protegen con un Mutex.
    let data = Arc::new(Mutex::new(0));

    // Se crea un canal que permite multiples productores y un simple consumidor.
    let (tx, rx) = sync::mpsc::channel();

    // Se crean los N threads
    for _ in 0..N {
        // Se crea una tupla y se desestructura (aunque es superfluo)!
        // Este ejemplo es de la doc oficial asi que sera una buena practica.
        //
        // Se crea un clone del Arc y un productor del canal para moverlos
        // al nuevo thread.
        let (data, tx) = (Arc::clone(&data), tx.clone());

        thread::spawn(move || {
            // The shared state can only be accessed once the lock is held.
            // Our non-atomic increment is safe because we're the only thread
            // which can access the shared state when the lock is held.
            //
            // We unwrap() the return value to assert that we are not expecting
            // threads to ever fail while holding the lock.
            let mut data = data.lock().unwrap();
            *data += 1;
            if *data == N {
                tx.send(()).unwrap();
            }
            // the lock is unlocked here when `data` goes out of scope.
        });
    }

    rx.recv().unwrap();
}
