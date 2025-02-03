use crate::HmacSha256;
use hmac::Mac;

pub struct Message {
    pub header: Vec<u8>,
    pub body: Vec<u8>,
    pub tag: Vec<u8>,
}

impl Message {
    pub fn verify_hmac(&self, key: &[u8]) -> bool {
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update([self.header.clone(), self.body.clone()].concat().as_slice());
        mac.verify_slice(self.tag.as_slice()).is_ok()
    }
}
