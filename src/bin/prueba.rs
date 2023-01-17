

#[tokio::main(flavor = "current_thread")]
async fn main() -> mini_redis::Result<()> {

    let _a = "1234".as_bytes();

    let _c = vec![1, 2, 3];
    let _d = &_c[..];

    println!("Hola");
    Ok(())
    
}
