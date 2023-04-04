package org.sunbird.auth.verifier;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.sunbird.JsonKey;
import org.sunbird.utils.PropertiesCache;
import org.sunbird.request.LoggerUtil;


public class KeyManager {

  private static LoggerUtil logger = new LoggerUtil(KeyManager.class);
  private static PropertiesCache propertiesCache = PropertiesCache.getInstance();

  private static Map<String, KeyData> keyMap = new HashMap<String, KeyData>();

  public static void init() {
    String basePath = propertiesCache.getProperty(JsonKey.ACCESS_TOKEN_PUBLICKEY_BASEPATH);
    logger.info("KeyManager:basePath"+basePath);
    try (Stream<Path> walk = Files.walk(Paths.get(basePath))) {
      logger.info("KeyManager:init: Start");
      List<String> result =
          walk.filter(Files::isRegularFile).map(x -> x.toString()).collect(Collectors.toList());
      result.forEach(
          file -> {
            try {
              StringBuilder contentBuilder = new StringBuilder();
              Path path = Paths.get(file);
              Files.lines(path, StandardCharsets.UTF_8)
                  .forEach(
                      x -> {
                        contentBuilder.append(x);
                      });
              KeyData keyData =
                  new KeyData(
                      path.getFileName().toString(), loadPublicKey(contentBuilder.toString()));
              keyMap.put(path.getFileName().toString(), keyData);
            } catch (Exception e) {
              logger.error("KeyManager:init: exception in reading public keys ", e);
            }
          });
    } catch (Exception e) {
      logger.error("KeyManager:init: exception in loading publickeys ", e);
    }
  }

  public static KeyData getPublicKey(String keyId) {
    return keyMap.get(keyId);
  }

  public static PublicKey loadPublicKey(String key) throws Exception {
    String publicKey = new String(key.getBytes(), StandardCharsets.UTF_8);
    publicKey = publicKey.replaceAll("(-+BEGIN PUBLIC KEY-+)", "");
    publicKey = publicKey.replaceAll("(-+END PUBLIC KEY-+)", "");
    publicKey = publicKey.replaceAll("[\\r\\n]+", "");
    byte[] keyBytes = Base64Util.decode(publicKey.getBytes("UTF-8"), Base64Util.DEFAULT);
    X509EncodedKeySpec X509publicKey = new X509EncodedKeySpec(keyBytes);
    KeyFactory kf = KeyFactory.getInstance("RSA");
    return kf.generatePublic(X509publicKey);
  }
}
