package org.sunbird.telemetry.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.common.util.LoggerEnum;
import org.sunbird.telemetry.JsonKey;
import org.sunbird.telemetry.TelemetryEnvKey;
import org.sunbird.telemetry.dto.Actor;
import org.sunbird.telemetry.dto.Context;
import org.sunbird.telemetry.dto.Producer;
import org.sunbird.telemetry.dto.Target;
import org.sunbird.telemetry.dto.Telemetry;

/**
 * class to transform the request data to telemetry events
 *
 * @author Arvind
 */
public class TelemetryGenerator {

  private static ObjectMapper mapper = new ObjectMapper();
  private static Logger logger = LoggerFactory.getLogger(TelemetryGenerator.class);

  private TelemetryGenerator() {}

  /**
   * To generate api_access LOG telemetry JSON string.
   *
   * @param context Map contains the telemetry context info like actor info, env info etc.
   * @param params Map contains the telemetry event data info
   * @return Telemetry event
   */
  public static String audit(Map<String, Object> context, Map<String, Object> params) {
    if (!validateRequest(context, params)) {
      return "";
    }
    String actorId = (String) context.get(JsonKey.ACTOR_ID);
    String actorType = (String) context.get(JsonKey.ACTOR_TYPE);
    Actor actor = new Actor(actorId, StringUtils.capitalize(actorType));
    Target targetObject =
        generateTargetObject((Map<String, Object>) params.get(JsonKey.TARGET_OBJECT));
    Context eventContext = getContext(context);
    // assign cdata into context from params correlated objects...
    if (params.containsKey(JsonKey.CORRELATED_OBJECTS)) {
      setCorrelatedDataToContext(params.get(JsonKey.CORRELATED_OBJECTS), eventContext);
    }

    // assign request id into context cdata ...
    String reqId = (String) context.get(JsonKey.REQUEST_ID);
    if (!StringUtils.isBlank(reqId)) {
      Map<String, Object> map = new HashMap<>();
      map.put(JsonKey.ID, reqId);
      map.put(JsonKey.TYPE, TelemetryEnvKey.REQUEST_UPPER_CAMEL);
      eventContext.getCdata().add(map);
    }

    Map<String, Object> edata = generateAuditEdata(params);

    Telemetry telemetry =
        new Telemetry(TelemetryEvents.AUDIT.getName(), actor, eventContext, edata);
    telemetry.setMid(reqId);
    return getTelemetry(telemetry);
  }

  private static void setCorrelatedDataToContext(Object correlatedObjects, Context eventContext) {
    ArrayList<Map<String, Object>> list = (ArrayList<Map<String, Object>>) correlatedObjects;
    ArrayList<Map<String, Object>> targetList = new ArrayList<>();
    if (null != list && !list.isEmpty()) {
      for (Map<String, Object> m : list) {
        if(m.get(JsonKey.ID) != null) {
          Map<String, Object> map = new HashMap<>();
          map.put(JsonKey.ID, m.get(JsonKey.ID));
          map.put(JsonKey.TYPE, StringUtils.capitalize((String) m.get(JsonKey.TYPE)));
          targetList.add(map);
        }
      }
    }
    eventContext.setCdata(targetList);
  }

  private static Target generateTargetObject(Map<String, Object> targetObject) {

    Target target =
        new Target(
            (String) targetObject.get(JsonKey.ID),
            (String) targetObject.get(JsonKey.TYPE));
    if (targetObject.get(JsonKey.ROLLUP) != null) {
      target.setRollup((Map<String, String>) targetObject.get(JsonKey.ROLLUP));
    }
    return target;
  }

  private static Map<String, Object> generateAuditEdata(Map<String, Object> params) {

    Map<String, Object> edata = new HashMap<>();
    Map<String, Object> props = (Map<String, Object>) params.get(JsonKey.PROPS);
    // TODO: need to rethink about this one .. if map is null then what to do
    if (null != props) {
      edata.put(JsonKey.PROPS, getProps(props));
    }

    Map<String, Object> target = (Map<String, Object>) params.get(JsonKey.TARGET_OBJECT);
    if (target.get(JsonKey.TYPE) != null) {
      edata.put(JsonKey.TYPE,(String) target.get(JsonKey.TYPE ));
    }

    if (target.get(JsonKey.STATE) != null) {
      edata.put(JsonKey.STATE,(String) target.get(JsonKey.STATE));
      if (JsonKey.UPDATE.equalsIgnoreCase((String) target.get(JsonKey.STATE))
          && edata.get(props) != null) {
        removeAttributes((Map<String, Object>) edata.get(props), JsonKey.ID);
      }
    }
    if (target.get(JsonKey.PREV_STATE) != null) {
      edata.put(JsonKey.PREVSTATE, (String) target.get(JsonKey.PREV_STATE));
      if (JsonKey.UPDATE.equalsIgnoreCase((String) target.get(JsonKey.PREV_STATE))
          && edata.get(props) != null) {
        removeAttributes((Map<String, Object>) edata.get(props), JsonKey.ID);
      }
    }
    return edata;
  }

  private static void removeAttributes(Map<String, Object> map, String... properties) {
    for (String property : properties) {
      map.remove(property);
    }
  }

  private static List<String> getProps(Map<String, Object> map) {
    try {
      return map.entrySet()
          .stream()
          .map(entry -> entry.getKey())
          .map(
              key -> {
                if (map.get(key) instanceof Map) {
                  List<String> keys = getProps((Map<String, Object>) map.get(key));
                  return keys.stream()
                      .map(childKey -> key + "." + childKey)
                      .collect(Collectors.toList());
                } else {
                  return Arrays.asList(key);
                }
              })
          .flatMap(List::stream)
          .collect(Collectors.toList());
    } catch (Exception e) {
      logger.error("getProps error = {} {} ", e, LoggerEnum.ERROR.name());
    }
    return new ArrayList<>();
  }

  private static Context getContext(Map<String, Object> context) {
    String channel = (String) context.get(JsonKey.CHANNEL);
    String env = (String) context.get(JsonKey.ENV);
    String did = (String) context.get(JsonKey.DEVICE_ID);
    Producer producer = getProducer(context);
    Context eventContext = new Context(channel, env, producer);
    eventContext.setDid(did);
    if (eventContext.getChannel() != null) {
      eventContext.setRollup(new HashMap<String,String>(){{put(JsonKey.L1, eventContext.getChannel());}});
    }
    return eventContext;
  }

  private static Producer getProducer(Map<String, Object> context) {
    String id = "";
    if (context != null && context.size() != 0) {
      if (StringUtils.isNotBlank((String) context.get(JsonKey.APP_ID))) {
        id = (String) context.get(JsonKey.APP_ID);
      } else {
        id = (String) context.get(JsonKey.PDATA_ID);
      }
      String pid = (String) context.get(JsonKey.PDATA_PID);
      String ver = (String) context.get(JsonKey.PDATA_VERSION);
      return new Producer(id, pid, ver);
    } else {
      return new Producer("", "", "");
    }
  }

  private static String getTelemetry(Telemetry telemetry) {
    String event = "";
    try {
      event = mapper.writeValueAsString(telemetry);
      logger.debug("getTelemetry = Telemetry Event :{} {} ", event, LoggerEnum.DEBUG.name());
    } catch (Exception e) {
      logger.error(
          "getTelemetry = Telemetry Event: failed to generate audit events: {} {}",
          e,
          LoggerEnum.ERROR.name());
    }
    return event;
  }

  /**
   * Method to generate the search type telemetry event.
   *
   * @param context Map contains the telemetry context info like actor info, env info etc.
   * @param params Map contains the telemetry event data info
   * @return Search Telemetry event
   */
  public static String search(Map<String, Object> context, Map<String, Object> params) {

    if (!validateRequest(context, params)) {
      return "";
    }
    String actorId = (String) context.get(JsonKey.ACTOR_ID);
    String actorType = (String) context.get(JsonKey.ACTOR_TYPE);
    Actor actor = new Actor(actorId, StringUtils.capitalize(actorType));

    Context eventContext = getContext(context);

    String reqId = (String) context.get(JsonKey.REQUEST_ID);
    if (!StringUtils.isBlank(reqId)) {
      Map<String, Object> map = new HashMap<>();
      map.put(JsonKey.ID, reqId);
      map.put(JsonKey.TYPE, TelemetryEnvKey.REQUEST_UPPER_CAMEL);
      eventContext.getCdata().add(map);
    }
    Map<String, Object> edata = generateSearchEdata(params);
    Telemetry telemetry =
        new Telemetry(TelemetryEvents.SEARCH.getName(), actor, eventContext, edata);
    telemetry.setMid(reqId);
    return getTelemetry(telemetry);
  }

  private static Map<String, Object> generateSearchEdata(Map<String, Object> params) {

    Map<String, Object> edata = new HashMap<>();
    String type = (String) params.get(JsonKey.TYPE);
    String query = (String) params.get(JsonKey.QUERY);
    Map filters = (Map) params.get(JsonKey.FILTERS);
    Map sort = (Map) params.get(JsonKey.SORT);
    List<Map> topn = (List<Map>) params.get(JsonKey.TOPN);

    edata.put(JsonKey.TYPE, StringUtils.capitalize(type));
    if (null == query) {
      query = "";
    }
    edata.put(JsonKey.QUERY, query);
    edata.put(JsonKey.FILTERS, filters);
    edata.put(JsonKey.SORT, sort);
    edata.put(JsonKey.SIZE, params.get(JsonKey.SIZE));
    edata.put(JsonKey.TOPN, topn);
    return edata;
  }

  /**
   * Method to generate the log type telemetry event.
   *
   * @param context Map contains the telemetry context info like actor info, env info etc.
   * @param params Map contains the telemetry event data info
   * @return Search Telemetry event
   */
  public static String log(Map<String, Object> context, Map<String, Object> params) {

    if (!validateRequest(context, params)) {
      return "";
    }
    String actorId = (String) context.get(JsonKey.ACTOR_ID);
    String actorType = (String) context.get(JsonKey.ACTOR_TYPE);
    Actor actor = new Actor(actorId, StringUtils.capitalize(actorType));

    Context eventContext = getContext(context);

    // assign request id into context cdata ...
    String reqId = (String) context.get(JsonKey.REQUEST_ID);
    if (!StringUtils.isBlank(reqId)) {
      Map<String, Object> map = new HashMap<>();
      map.put(JsonKey.ID, reqId);
      map.put(JsonKey.TYPE, TelemetryEnvKey.REQUEST_UPPER_CAMEL);
      eventContext.getCdata().add(map);
    }

    Map<String, Object> edata = generateLogEdata(params);
    Telemetry telemetry = new Telemetry(TelemetryEvents.LOG.getName(), actor, eventContext, edata);
    telemetry.setMid(reqId);
    return getTelemetry(telemetry);
  }

  private static Map<String, Object> generateLogEdata(Map<String, Object> params) {

    Map<String, Object> edata = new HashMap<>();
    String logType = (String) params.get(JsonKey.LOG_TYPE);
    String logLevel = (String) params.get(JsonKey.LOG_LEVEL);
    String message = (String) params.get(JsonKey.MESSAGE);

    edata.put(JsonKey.TYPE, StringUtils.capitalize(logType));
    edata.put(JsonKey.LEVEL, logLevel);
    edata.put(JsonKey.MESSAGE, message != null ? message : "");

    edata.put(
        JsonKey.PARAMS,
        getParamsList(params, Arrays.asList(JsonKey.LOG_TYPE, JsonKey.LOG_LEVEL, JsonKey.MESSAGE)));
    return edata;
  }

  private static List<Map<String, Object>> getParamsList(
      Map<String, Object> params, List<String> ignore) {
    List<Map<String, Object>> paramsList = new ArrayList<Map<String, Object>>();
    if (null != params && !params.isEmpty()) {
      for (Entry<String, Object> entry : params.entrySet()) {
        if (!ignore.contains(entry.getKey())) {
          Map<String, Object> param = new HashMap<String, Object>();
          param.put(entry.getKey(), entry.getValue());
          paramsList.add(param);
        }
      }
    }
    return paramsList;
  }

  /**
   * Method to generate the error type telemetry event.
   *
   * @param context Map contains the telemetry context info like actor info, env info etc.
   * @param params Map contains the error event data info
   * @return Search Telemetry event
   */
  public static String error(Map<String, Object> context, Map<String, Object> params) {

    if (!validateRequest(context, params)) {
      return "";
    }
    String actorId = (String) context.get(JsonKey.ACTOR_ID);
    String actorType = (String) context.get(JsonKey.ACTOR_TYPE);
    Actor actor = new Actor(actorId, StringUtils.capitalize(actorType));

    Context eventContext = getContext(context);

    // assign request id into context cdata ...
    String reqId = (String) context.get(JsonKey.REQUEST_ID);
    if (!StringUtils.isBlank(reqId)) {
      Map<String, Object> map = new HashMap<>();
      map.put(JsonKey.ID, reqId);
      map.put(JsonKey.TYPE, TelemetryEnvKey.REQUEST_UPPER_CAMEL);
      eventContext.getCdata().add(map);
    }

    Map<String, Object> edata = generateErrorEdata(params);
    Telemetry telemetry =
        new Telemetry(TelemetryEvents.ERROR.getName(), actor, eventContext, edata);
    telemetry.setMid(reqId);
    return getTelemetry(telemetry);
  }

  private static Map<String, Object> generateErrorEdata(Map<String, Object> params) {
    Map<String, Object> edata = new HashMap<>();
    String error = (String) params.get(JsonKey.ERR);
    String errorType = (String) params.get(JsonKey.ERR_TYPE);
    String stackTrace = (String) params.get(JsonKey.STACKTRACE);
    edata.put(JsonKey.ERR , error);
    edata.put(JsonKey.ERR_TYPE, errorType);
    edata.put(JsonKey.STACKTRACE, getFirstNCharacterString(stackTrace, 100));
    return edata;
  }

  private static boolean validateRequest(Map<String, Object> context, Map<String, Object> params) {

    boolean flag = true;
    if (null == context || context.isEmpty() || params == null || params.isEmpty()) {
      flag = false;
    }
    return flag;
  }

  private static String getFirstNCharacterString(String originalText, int noOfChar) {
    if (StringUtils.isBlank(originalText)) {
      return "";
    }
    String firstNChars = "";
    if (originalText.length() > noOfChar) {
      firstNChars = originalText.substring(0, noOfChar);
    } else {
      firstNChars = originalText;
    }
    return firstNChars;
  }
}
