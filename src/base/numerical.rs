pub fn is_power_of_two(num: u32) -> bool {
    num != 0 && (num & (num - 1)) == 0
}
