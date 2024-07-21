pub const WALFRAME_HEADER_SIZE: u64 = 24;
pub const WAL_HEADER_SIZE: usize = 32;

pub const WAL_HEADER_BIG_ENDIAN_MAGIC: [u8; 4] = [0x37, 0x7f, 0x06, 0x83];
pub const WAL_HEADER_LITTLE_ENDIAN_MAGIC: [u8; 4] = [0x37, 0x7f, 0x06, 0x82];

// implementation of sqlite check algorithm
pub fn checksum(data: &[u8], is_big_endian: bool) -> (u32, u32) {
    let mut i = 0;
    let mut s1: u32 = 0;
    let mut s2: u32 = 0;
    while i < data.len() {
        let bytes1 = &data[i..i + 4];
        let bytes2 = &data[i + 4..i + 8];
        let (n1, n2) = if is_big_endian {
            (
                u32::from_be_bytes(bytes1.try_into().unwrap()),
                u32::from_be_bytes(bytes2.try_into().unwrap()),
            )
        } else {
            (
                u32::from_le_bytes(bytes1.try_into().unwrap()),
                u32::from_le_bytes(bytes2.try_into().unwrap()),
            )
        };
        // use `wrapping_add` instead of `+` directly, or else will be overflow panic
        s1 = s1.wrapping_add(n1).wrapping_add(s2);
        s2 = s2.wrapping_add(n2).wrapping_add(s1);

        i += 8;
    }

    (s1, s2)
}
