
fn stringify(x: u32) -> String { 
    format!("error code: {x}") 
}

fn main() {

    let x: Result<u32, u32> = Result::Ok(2);
    let x_2 = x.map_err(stringify);
    let y_2 = Result::Ok(2);
    assert_eq!(x_2, y_2);
    
    let x: Result<u32, u32> = Result::Err(13);
    let x_2 = x.map_err(stringify);
    let y_2 : Result<u32, String> = Result::Err("error code: 13".to_string());
    assert_eq!(x_2, y_2);
    
}
