package com.td.gwiro.wires.messaging;

import com.td.gwiro.wires.config.AclConfig;
import com.td.gwiro.wires.config.AuthConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic entitlementTopicCreation(AuthConfig config) {
        return new NewTopic(config.getEntitlementsTopic(), 1, (short) 1);
    }

    @Bean
    public NewTopic entitlementRoleTopicCreation(AuthConfig config) {
        return new NewTopic(config.getRoleEntitlementsTopic(), 1, (short) 1);
    }

    @Bean
    public NewTopic accountTopicCreation(AclConfig config) {
        return new NewTopic(config.getAccountsTopic(), 1, (short) 1);
    }

    @Bean
    public NewTopic userTopicCreation(AclConfig config) {
        return new NewTopic(config.getUsersTopic(), 1, (short) 1);
    }

    @Bean
    public NewTopic customerTopicCreation(AclConfig config) {
        return new NewTopic(config.getCustomersTopic(), 1, (short) 1);
    }

    @Bean
    public NewTopic paymentStatusTopicCreation(AclConfig config) {
        return new NewTopic(config.getPaymentStatusTopic(), 1, (short) 1);
    }
}