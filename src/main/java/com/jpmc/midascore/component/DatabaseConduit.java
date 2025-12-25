package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DatabaseConduit {
    private final UserRepository userRepository;

    public DatabaseConduit(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas")
    public void onTransaction(Transaction transaction) {
        System.out.println("Received Transaction Amount: " + transaction.getAmount());
    }

}
