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
    logger.info("Email List in cc : " + emailReq.getCc() + "Email List in To : " + emailReq.getTo());
    if (emailReq == null) {
      logger.info(context, "Email request is null or empty:");
      return false;
      // either email object has bcc or to list size more than 1 then pass it as bcc.
    } else if (CollectionUtils.isNotEmpty(emailReq.getBcc()) && emailReq.getTo().size() > 1) {
      logger.info("Into bCC condition block");
      return email.sendEmail(
          email.getFromEmail(),
          emailReq.getSubject(),
          emailReq.getBody(),
          CollectionUtils.isEmpty(emailReq.getBcc()) ? emailReq.getTo() : emailReq.getBcc());
    } else if (CollectionUtils.isNotEmpty(emailReq.getCc()) && emailReq.getTo().size() > 1) {
      logger.info("Into CC condition block");
      return email.sendMail(
          emailReq.getTo(), emailReq.getSubject(), emailReq.getBody(), emailReq.getCc());
    } else {
      logger.info("Into block without cc or bcc");
      return email.sendMail(emailReq.getTo(), emailReq.getSubject(), emailReq.getBody());
    }
  }
}
