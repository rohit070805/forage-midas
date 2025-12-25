package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

@Component
public class DatabaseConduit {
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final RestTemplate restTemplate = new RestTemplate(); // REST Client

    public DatabaseConduit(UserRepository userRepository, TransactionRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas")
    @Transactional
    public void onTransaction(Transaction transaction) {
        long senderId = transaction.getSenderId();
        long recipientId = transaction.getRecipientId();
        float amount = transaction.getAmount();

        UserRecord sender = userRepository.findById(senderId);
        UserRecord recipient = userRepository.findById(recipientId);

        if (sender != null && recipient != null && sender.getBalance() >= amount) {
            // 1. Call the Incentive API
            Incentive incentive = restTemplate.postForObject("http://localhost:8080/incentive", transaction, Incentive.class);
            float incentiveAmount = incentive.getAmount();

            // 2. Update Balances
            // Sender loses 'amount'
            sender.setBalance(sender.getBalance() - amount);
            
            // Recipient gets 'amount' + 'incentiveAmount'
            recipient.setBalance(recipient.getBalance() + amount + incentiveAmount);

            userRepository.save(sender);
            userRepository.save(recipient);

            // 3. Save Transaction with Incentive
            TransactionRecord record = new TransactionRecord(sender, recipient, amount);
            record.setIncentive(incentiveAmount);
            transactionRepository.save(record);

            // LOGGING FOR TASK 4 ANSWER
            if (sender.getName().equals("wilbur")) {
                System.out.println("WILBUR BALANCE: " + sender.getBalance());
            }
            if (recipient.getName().equals("wilbur")) {
                System.out.println("WILBUR BALANCE: " + recipient.getBalance());
            }
        } else {
            System.out.println("Invalid transaction discarded.");
        }
    }
}