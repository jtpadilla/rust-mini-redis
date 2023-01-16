
#![feature(concat_bytes)]
pub const MY_CONST: &[u8; 256] = concat_bytes!(b"abcdef", [0; 250]);

#[tokio::main(flavor = "current_thread")]
async fn main() -> mini_redis::Result<()> {

    println!("{:?}", &MY_CONST[0..10]);

    let _a = "1234".as_bytes();

    let _c = vec![1, 2, 3];
    let _d = &_c[..];

    println!("Hola");
    Ok(())
    
}
