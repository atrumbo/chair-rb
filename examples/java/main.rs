use std::process::Command;

fn main() {
    println!("Delegating to gradle for Java example");

    let status = if cfg!(target_os = "windows") {
        Command::new("cmd")
            .args(&["/C", "gradlew", "run"])
            .current_dir("examples/java/")
            .status()
            .expect("failed to execute process")
    } else {
        Command::new("sh")
            .arg("gradlew")
            .arg("run")
            .current_dir("examples/java/")
            .status()
            .expect("failed to execute process")
    };

    assert!(status.success())
}