
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Base64;

public class Base64ToJKSConverter {
    public String base64ToJKS(String base64String, String keystorePassword) {
        try {
            byte[] decodedBytes = Base64.getDecoder().decode(base64String);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(decodedBytes);
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(inputStream, keystorePassword.toCharArray());

            // Write keystore to a temporary file
            File tempFile = File.createTempFile("schema", ".jks");
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                keyStore.store(fos, keystorePassword.toCharArray());
                return tempFile.getAbsolutePath();
            } catch (IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
                e.printStackTrace();
            }
        } catch (IllegalArgumentException | IOException | KeyStoreException | NoSuchAlgorithmException |
                 CertificateException e) {
            e.printStackTrace();
        }
        return null; // Return null if an error occurs
    }
}
