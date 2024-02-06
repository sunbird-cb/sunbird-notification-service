/** */
package org.sunbird.notification.email.service.impl;

import org.apache.commons.collections.CollectionUtils;
import org.sunbird.notification.beans.EmailConfig;
import org.sunbird.notification.beans.EmailRequest;
import org.sunbird.notification.email.Email;
import org.sunbird.notification.email.service.IEmailService;
import org.sunbird.request.LoggerUtil;

import java.util.Map;

/** @author manzarul */
public class SmtpEMailServiceImpl implements IEmailService {
  private static LoggerUtil logger = new LoggerUtil(SmtpEMailServiceImpl.class);
  private Email email = null;

  public SmtpEMailServiceImpl() {
    email = Email.getInstance();
  }

  public SmtpEMailServiceImpl(EmailConfig config) {
    email = Email.getInstance(config);
  }

  @Override
  public boolean sendEmail(EmailRequest emailReq, Map<String,Object> context) {
    logger.debug("Email List in cc : " + emailReq.getCc() + "Email List in To : " + emailReq.getTo());
    if (emailReq == null) {
      logger.info(context, "Email request is null or empty:");
      return false;
    } else {
      logger.info("Email Request To List : " + emailReq.getTo());
      logger.info("Email Request cc List : " + emailReq.getCc());
      logger.info("Email Request bcc List : " + emailReq.getBcc());
      return email.sendMail(emailReq.getTo(), emailReq.getSubject(), emailReq.getBody(), emailReq.getCc(), emailReq.getBcc());
    }
  }
}
