fn main() {

    // Se crea un vector de caracteres y se carga
    let vec = vec!['a', 'b', 'c'];

    // Se creo un iterador para extraer cada una de las entradas del vector
    let mut into_iter = vec.into_iter();

    // La estructura 'IntoIter' implementa el metodo `as_slice` que retorna un Slice 
    // con las entradas pendientes de consumir.
    //
    // Se verifica las entradas que quedan por consumir.
    assert_eq!(into_iter.as_slice(), &['a', 'b', 'c']);

    // La estructura 'IntoIter' entre otro traits implementa `Iterator` donde 
    // entre otros metodos tenemos `next`.
    let _ = into_iter.next().unwrap();

    // Se verifica que se ha consumido el primer mensaje
    assert_eq!(into_iter.as_slice(), &['b', 'c']);

    println!("Todo Ok!");

}
