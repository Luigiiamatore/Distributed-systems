use hmac::{Hmac, Mac};
use rustls::{
    pki_types::{pem::PemObject, ServerName},
    ClientConnection, RootCertStore, ServerConnection, StreamOwned,
};
use sha2::Sha256;
use std::{
    io::{Read, Write},
    sync::Arc,
};

pub struct SecureClient<L: Read + Write> {
    stream: StreamOwned<ClientConnection, L>,
    hmac_key: Vec<u8>,
}

pub struct SecureServer<L: Read + Write> {
    stream: StreamOwned<ServerConnection, L>,
    hmac_key: Vec<u8>,
}

impl<L: Read + Write> SecureClient<L> {
    /// Creates a new instance of SecureClient.
    ///
    /// SecureClient communicates with SecureServer via `link`.
    /// The messages include a HMAC tag calculated using `hmac_key`.
    /// A certificate of SecureServer is signed by `root_cert`.
    /// We are connecting with `server_hostname`.
    pub fn new(
        link: L,
        hmac_key: &[u8],
        root_cert: &str,
        server_hostname: ServerName<'static>,
    ) -> Self {
        Self {
            stream: Self::setup_connection(link, root_cert, server_hostname),
            hmac_key: hmac_key.to_vec(),
        }
    }

    fn setup_connection(link: L, root_cert: &str, server_hostname: ServerName<'static>) -> StreamOwned<ClientConnection, L> {
        let mut root_store = RootCertStore::empty();
        root_store.add_parsable_certificates(rustls::pki_types::CertificateDer::from_pem_slice(
            root_cert.as_bytes(),
        ));

        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let connection = ClientConnection::new(Arc::new(client_config), server_hostname).unwrap();

        StreamOwned::new(connection, link)
    }

    /// Sends the data to the server. The sent message follows the
    /// format specified in the description of the assignment.
    pub fn send_msg(&mut self, data: Vec<u8>) {
        let message_length = data.len() as u32;
        let tag = Self::calculate_hmac_tag(self.hmac_key.as_slice(), &data);
        let full_message = vec![
            &message_length.to_be_bytes(),
            data.as_slice(),
            tag.as_slice(),
        ].concat();

        self.stream.write_all(full_message.as_slice()).unwrap();
    }

    fn calculate_hmac_tag(hmac_key: &[u8], message: &Vec<u8>) -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(hmac_key).unwrap();

        mac.update(message);

        let tag = mac.finalize().into_bytes();
        tag.into()
    }
}

impl<L: Read + Write> SecureServer<L> {
    /// Creates a new instance of SecureServer.
    ///
    /// SecureServer receives messages from SecureClients via `link`.
    /// HMAC tags of the messages are verified against `hmac_key`.
    /// The private key of the SecureServer's certificate is `server_private_key`,
    /// and the full certificate chain is `server_full_chain`.
    pub fn new(
        link: L,
        hmac_key: &[u8],
        server_private_key: &str,
        server_full_chain: &str,
    ) -> Self {
        Self {
            stream: Self::setup_connection(server_full_chain, link, server_private_key),
            hmac_key: hmac_key.to_vec(),
        }
    }

    fn setup_connection(server_full_chain: &str, link: L, server_private_key: &str) -> StreamOwned<ServerConnection, L> {
        let certs = rustls::pki_types::CertificateDer::pem_slice_iter(server_full_chain.as_bytes())
            .flatten()
            .collect();

        let private_key =
            rustls::pki_types::PrivateKeyDer::from_pem_slice(server_private_key.as_bytes())
                .unwrap();

        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, private_key)
            .unwrap();

        let connection = ServerConnection::new(Arc::new(server_config)).unwrap();
        StreamOwned::new(connection, link)
    }

    /// Receives the next incoming message and returns the message's content
    /// (i.e., without the message size and without the HMAC tag) if the
    /// message's HMAC tag is correct. Otherwise, returns `SecureServerError`.
    pub fn recv_message(&mut self) -> Result<Vec<u8>, SecureServerError> {
        let mut buffer_message_length = [0u8; 4];
        self.stream.read_exact(&mut buffer_message_length).unwrap();

        let message_length = u32::from_be_bytes(buffer_message_length);

        let mut buffer_data = vec![0u8; message_length as usize];
        self.stream.read_exact(&mut buffer_data).unwrap();

        let mut buffer_hmac = [0u8; 32];
        self.stream.read_exact(&mut buffer_hmac).unwrap();

        if Self::verify_hmac_tag(&buffer_hmac, &self.hmac_key, &buffer_data) {
            Ok(buffer_data)
        } else {
            Err(SecureServerError::InvalidHmac)
        }
    }

    fn verify_hmac_tag(tag: &[u8], hmac_key: &[u8], message: &Vec<u8>) -> bool {
        let mut mac = HmacSha256::new_from_slice(hmac_key).unwrap();

        mac.update(message);

        mac.verify_slice(&tag).is_ok()
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum SecureServerError {
    /// The HMAC tag of a message is invalid.
    InvalidHmac,
}

// Create a type alias:
type HmacSha256 = Hmac<Sha256>;
