use std::env;

fn main() {
    let config_file_path = env::args()
        .nth(1)
        .expect("You need to provide a an argument for the path");
    
    let config = bench_suite_config::BenchSuiteTasks::new(&config_file_path).unwrap();

    let mut count = 0;
    for i in config.to_collect(){
        println!("{:?}",i);
        count +=1;
    }


    println!("{}",count);

}
