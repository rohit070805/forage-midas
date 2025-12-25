package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class DatabaseConduit {
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;

    public DatabaseConduit(UserRepository userRepository, TransactionRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas")
    @Transactional // Ensures database operations are atomic
    public void onTransaction(Transaction transaction) {
        long senderId = transaction.getSenderId();
        long recipientId = transaction.getRecipientId();
        float amount = transaction.getAmount();

        UserRecord sender = userRepository.findById(senderId);
        UserRecord recipient = userRepository.findById(recipientId);

        // Validation: Check if both users exist and sender has enough balance
        if (sender != null && recipient != null && sender.getBalance() >= amount) {
            
            // 1. Deduct from sender
            sender.setBalance(sender.getBalance() - amount);
            userRepository.save(sender);

            // 2. Add to recipient
            recipient.setBalance(recipient.getBalance() + amount);
            userRepository.save(recipient);

            // 3. Record the transaction
            TransactionRecord record = new TransactionRecord(sender, recipient, amount);
            transactionRepository.save(record);
            
            System.out.println("Processed transaction: " + amount + " from " + senderId + " to " + recipientId);
        } else {
            System.out.println("Invalid transaction discarded: " + transaction);
        }
    }
}