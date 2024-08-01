use std::fmt::Write;

/// Mask a string by "******", but keep `unmask_len` of suffix.
#[inline]
pub fn mask_string(s: &str, unmask_len: usize) -> String {
    if s.len() <= unmask_len {
        s.to_string()
    } else {
        let mut ret = "******".to_string();
        ret.push_str(&s[(s.len() - unmask_len)..]);
        ret
    }
}

pub fn u8_array_as_hex(arr: &[u8]) -> String {
    let hex_str: String = arr
        .iter()
        .map(|byte| format!("0x{:02X}", byte))
        .collect::<Vec<_>>()
        .join(" ");

    hex_str
}

pub fn format_integer_with_leading_zeros(num: u32, min_length: usize) -> String {
    let mut result = String::new();
    write!(&mut result, "{:0>width$}", num, width = min_length).unwrap();
    result
}
