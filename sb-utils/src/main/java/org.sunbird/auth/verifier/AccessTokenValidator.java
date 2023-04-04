package org.sunbird.auth.verifier;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.JsonKey;
import org.sunbird.utils.PropertiesCache;
import org.sunbird.request.LoggerUtil;


public class AccessTokenValidator {

  private static LoggerUtil logger = new LoggerUtil(AccessTokenValidator.class);
  private static ObjectMapper mapper = new ObjectMapper();
  private static PropertiesCache propertiesCache = PropertiesCache.getInstance();

  private static Map<String, Object> validateToken(String token) throws IOException {
    String[] tokenElements = token.split("\\.");
    String header = tokenElements[0];
    String body = tokenElements[1];
    String signature = tokenElements[2];
    String payLoad = header + JsonKey.DOT_SEPARATOR + body;
    Map<Object, Object> headerData =
        mapper.readValue(new String(decodeFromBase64(header)), Map.class);
    String keyId = headerData.get("kid").toString();
    boolean isValid =
        CryptoUtil.verifyRSASign(
            payLoad,
            decodeFromBase64(signature),
            KeyManager.getPublicKey(keyId).getPublicKey(),
            JsonKey.SHA_256_WITH_RSA);
    if (isValid) {
      Map<String, Object> tokenBody =
          mapper.readValue(new String(decodeFromBase64(body)), Map.class);
      boolean isExp = isExpired((Integer) tokenBody.get("exp"));
      if (isExp) {
        return Collections.EMPTY_MAP;
      }
      return tokenBody;
    }
    return Collections.EMPTY_MAP;
  }

  /**
   * managedtoken is validated and requestedByUserID is validated against the managedEncToken
   *
   * @param managedEncToken
   * @param requestedByUserId
   * @return
   */
  public static String verifyManagedUserToken(String managedEncToken, String requestedByUserId) {
    String managedFor = JsonKey.UNAUTHORIZED;
    try {
      Map<String, Object> payload = validateToken(managedEncToken);
      if (MapUtils.isNotEmpty(payload)) {
        String parentId = (String) payload.get(JsonKey.PARENT_ID);
        String muaId = (String) payload.get(JsonKey.SUB);
        logger.info(
            "AccessTokenValidator: parent uuid: "
                + parentId
                + " managedBy uuid: "
                + muaId
                + " requestedByUserID: "
                + requestedByUserId);
        boolean isValid = parentId.equalsIgnoreCase(requestedByUserId);
        if (isValid) {
          managedFor = muaId;
        }
      }
    } catch (Exception ex) {
      logger.error("Exception in AccessTokenValidator: verify ",ex);
      ex.printStackTrace();
    }
    return managedFor;
  }

  public static String verifyUserToken(String token) {
    String userId = JsonKey.UNAUTHORIZED;
    try {
      Map<String, Object> payload = validateToken(token);
      if (MapUtils.isNotEmpty(payload) && checkIss((String) payload.get("iss"))) {
        userId = (String) payload.get(JsonKey.SUB);
        if (StringUtils.isNotBlank(userId)) {
          int pos = userId.lastIndexOf(":");
          userId = userId.substring(pos + 1);
        }
      }
    } catch (Exception ex) {
      logger.error("Exception in verifyUserAccessToken: verify ", ex);
    }
    return userId;
  }

  private static boolean checkIss(String iss) {
    String realmUrl =
        propertiesCache.getProperty(JsonKey.SUNBIRD_SSO_URL)
            + "realms/"
            + propertiesCache.getProperty(JsonKey.SUNBIRD_SSO_REALM);
    return (realmUrl.equalsIgnoreCase(iss));
  }

  private static boolean isExpired(Integer expiration) {
    return (Time.currentTime() > expiration);
  }

  private static byte[] decodeFromBase64(String data) {
    return Base64Util.decode(data, 11);
  }
}
