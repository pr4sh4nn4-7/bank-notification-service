package com.banking.notificationservice.Service;

import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor

public class NotificationService {

  @KafkaListener(topics = "transaction.otp.generated")
  public void consumeOtpGenerated(
      @Payload Map<String, Object> payload

  ) {
    try {
      String accountNumber = payload.get("accountNumber").toString();
      String otp = payload.get("otp").toString();
      String transactionId = payload.get("transactionId").toString();
      String amount = payload.get("amount").toString();
      String reason = payload.get("reason").toString();

      sendAlert(
          accountNumber,
          "TRANSACTION VERIFICATION REQUIRED",
          String.format("Suspicious activity detected on you account, " +
              "reason: %s " + "A transaction of %s is pending verification. " +
              "Your OTP is: %s. Valid for 5 minutes." + "if this is not you ignore this message.", reason,
              transactionId, otp));

    } catch (Exception e) {
      log.error("Error sending otp notification : {}", e.getMessage());
    }

  }

  @KafkaListener(topics = "transaction.completed")
  public void consumeTransactionCompleted(
      @Payload Map<String, Object> payload) {
    try {
      String senderAcccount = payload.get("senderAccountNumber").toString();
      String receiverAccount = payload.get("receiverAccountNumber").toString();
      String amount = payload.get("amount").toString();
      // debit alert
      sendAlert(senderAcccount, "DEBIT ALERT",
          String.format("%s debited from account %s", amount, senderAcccount));

      // credit alert

      sendAlert(receiverAccount, "CREDIT ALERT",
          String.format("%s credited from account %s", amount, receiverAccount));
    } catch (Exception e) {
      log.error("Error sending trnasaction notification: {}", e.getMessage());
    }

  }

  @KafkaListener(topics = "fraud.detected")
  public void consumeFraudDetected(
      @Payload Map<String, Object> payload) {
    try {
      String accountNumber = payload.get("accountNumber").toString();
      String reason = payload.get("reason").toString();

      sendAlert(accountNumber, "SUSPICIOUS ACTIVITY DETECTED",
          String.format("Your account %s has been blocked. Reason: %s" +
              "Please contact your bank immediately", accountNumber, reason));

    } catch (Exception e) {

      log.error("Error sending freaud detection notification: {}", e.getMessage());
    }

  }

  @KafkaListener(topics = "transaction.refunded")
  public void consumeTransactionRefunded(
      @Payload Map<String, Object> payload

  ) {

    try {

      String senderAcccount = payload.get("senderAccountNumber").toString();
      String amount = payload.get("amount").toString();
      String reason = payload.get("reason").toString();

      sendAlert(senderAcccount, "REFUND PROCEED",
          String.format("Your transaction of %s was cancelled." +
              "Reason: %s" + "%s has been refunded to account %s",
              amount,
              reason,
              amount,
              senderAcccount

          )

      );

    } catch (Exception e) {

      log.error("Error sending refund procceed  notification: {}", e.getMessage());
    }

  }

  @KafkaListener(topics = "payment.completed")
  public void consumePaymentCompleted(
      @Payload Map<String, Object> payload

  ) {

    try {

      String accountNumber = payload.get("accountNumber").toString();
      String amount = payload.get("amount").toString();
      String reason = payload.get("reason").toString();

      sendAlert(accountNumber, "PAYMENT SUCCESSFUL",
          String.format("Payment of %s completed. Razorpay ID: %s",
              amount,
              payload.get("razorpayPaymentId")

          )

      );

    } catch (Exception e) {

      log.error("Error sending payment completed  notification failed: {}", e.getMessage());
    }

  }

  @KafkaListener(topics = "payment.failed")
  public void consumePaymentFailed(
      @Payload Map<String, Object> payload

  ) {

    try {

      String accountNumber = payload.get("accountNumber").toString();
      String amount = payload.get("amount").toString();
      String reason = payload.get("reason").toString();
      String paymentId = payload.get("paymentId").toString();

      sendAlert(accountNumber, "PAYMENT Failed",
          String.format("Payment of %s failed.Reason: %s . paymentId ID: %s. Try again later or contact us",
              amount,
              reason,
              paymentId

          )

      );

    } catch (Exception e) {

      log.error("Error sending payment failed  notification : {}", e.getMessage());
    }

  }

  private void sendAlert(String AccountNumber, String subject, String message) {
    log.info("...................................................");
    log.info("Account: {}", AccountNumber);
    log.info("Subject: {}", subject);
    log.info("message: {}", message);

  }

}
