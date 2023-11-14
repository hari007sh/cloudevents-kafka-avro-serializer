package com.td.gwiro.wires.messaging;

import static com.td.gwiro.wires.TestData.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.build2manage.time.ZonedTime;
import com.td.gwiro.acl.messaging.dto.SignatureStatus;
import com.td.gwiro.wires.config.ReferenceData;
import com.td.gwiro.wires.web.dto.*;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mapstruct.factory.Mappers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

import com.ibm.build2manage.kafka.KafkaTest;
import com.ibm.build2manage.kafka.PublishSpecification;
import com.td.gwiro.execution.messaging.dto.PaymentResponse;
import com.td.gwiro.wires.Application;
import com.td.gwiro.wires.mapping.WireMapper;
import com.td.gwiro.wires.repos.Wire;
import com.td.gwiro.wires.repos.WireRepository;

@KafkaTest
@ActiveProfiles({"messaging", "unit"})
@ConditionalOnProperty("test.run.kafkaIT")
@EnableAutoConfiguration
@SpringBootTest(classes = Application.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.cloud.vault.enabled=false",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "auth.domain=wires" // We need to use wires to match excel which doesn't support "unit" domain
})
class KafkaListenerIT {

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @Autowired
    ConcurrentKafkaListenerContainerFactory<Object, Object> factory;

    @MockBean
    private Clock clock;

    @SpyBean
    private ReferenceData referenceData;

    @Autowired
    private KafkaTemplate<Object, Object> kafka;

    @Value("${acl.payment-status-topic}")
    private String payment;

    @Value("${acl.signatures-topic}")
    private String signatures;

    private PaymentStatusListener paymentStatusListener;

    private WireMapper wireMapper;

    @Autowired
    private WireRepository wires;

    @BeforeEach
    void init() {
        Mockito.lenient().when(clock.instant()).thenReturn(TODAY.toInstant());
        Mockito.lenient().when(clock.getZone()).thenReturn(ZoneId.of("US/Eastern"));

        List<ReferenceData.DailyCutOffTime> cutoffs = List.of(
                new ReferenceData.DailyCutOffTime(
                        "domestic",
                        List.of("personal", "non-personal"),
                        "wire payment",
                        ZonedTime.parse("17:15:00EST"),
                        ZonedTime.parse("08:00:00EST"),
                        ZonedTime.parse("17:00:00EST")
                ),
                new ReferenceData.DailyCutOffTime(
                        "domestic",
                        List.of("personal", "non-personal"),
                        "time deposit closing",
                        ZonedTime.parse("17:15:00EST"),
                        ZonedTime.parse("08:00:00EST"),
                        ZonedTime.parse("17:00:00EST")
                ),
                new ReferenceData.DailyCutOffTime(
                        "domestic",
                        List.of("personal", "non-personal"),
                        "fed tax payment",
                        ZonedTime.parse("17:15:00EST"),
                        ZonedTime.parse("08:00:00EST"),
                        ZonedTime.parse("17:00:00EST")
                ));
        Mockito.when(referenceData.getDailyCutOffTimes()).thenReturn(cutoffs);
    }

    @CsvSource({
            "pending_transfer,pending,received_for_processing,false,update",
            "received_for_processing,processed,processed,true,processed",
            "pending_transfer,failed,transfer_failed,false,update",
            "pending_transfer,error,transfer_failed,false,update",
            "pending_transfer,bad_request,transfer_failed,false,update",
            "pending_transfer,failed,transfer_failed,false,update",
            "pending_transfer,error,transfer_failed,false,update",
            "pending_transfer,bad_request,transfer_failed,false,update"
    })
    @ParameterizedTest
    void sendPaymentStatusToKafka(String current, String status, String expected, boolean effectiveDateUpdated, String eventType) {
        UUID wireId = UUID.randomUUID();
        wires.save((Wire) create(1, wireId, "domestic", "Bob", "Julia", current).approvedBy(APPROVAL_EVENT).assignedApprover(APPROVAL_EVENT).addActivityItem(APPROVAL_EVENT));
        PaymentResponse paymentResponse = new PaymentResponse();
        paymentResponse.setInitiator(Map.of("id", wireId.toString()));
        paymentResponse.setStatus(status.toUpperCase());
        paymentResponse.setFedReferenceNumber("20200323C1B76D1C003155");
        paymentResponse.setMechanismType("WIRE");
        PublishSpecification.given(payment)
                .header("id", wireId.toString())
                .key(wireId.toString())
                .value(paymentResponse)
                .send(kafka)
                .then(registry, factory)
                .waitAtMost(30, TimeUnit.SECONDS);

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> !Objects.equals(wires.findById(wireId).get().getStatus(), current));

        Wire updatedWire = wires.findById(wireId).get();
        assertEquals(expected, updatedWire.getStatus());
        assertEquals(APPROVAL_EVENT, updatedWire.getActivity().get(1));
        assertEquals(APPROVAL_EVENT, updatedWire.getApprovedBy());
        assertEquals(APPROVAL_EVENT, updatedWire.getAssignedApprover());

        assertEquals(new Event().type(eventType).timestamp(TODAY), updatedWire.getActivity().get(updatedWire.getActivity().size() - 1));
        if (effectiveDateUpdated) {
            assertEquals(TODAY.toLocalDate(), updatedWire.getEffectiveDate());
        }
    }


    private static Wire create(int index, UUID wireId, String type, String creator, String originator, String status) {
        Wire result = new Wire();
        result.setId(wireId);
        result.setCreator(new Employee().id(UUID.randomUUID().toString()).fullName(creator));
        result.setPurpose("wire payment");
        result.setOriginator(new Customer()
                .rmNumber("000-000-000-000-" + index)
                .fullName(originator)
                .addressLines(new ArrayList<>(0))
                .type("personal"));
        result.setExecution(new Execution()
                .referenceNumber("MAX-0000000" + index));
        result.setAmount(BigDecimal.valueOf(index * 1000L + 500).setScale(2, RoundingMode.HALF_UP));
        result.setActivity(new ArrayList<>(List.of(CREATION_EVENT)));
        result.setOriginatorIdentification(new ArrayList<>(0));
        result.setType(type);
        result.setEffectiveDate(LocalDate.of(2022, 8, 18 + index));
        result.setStatus(status);
        result.setDepartment("RETAIL");
        return result;
    }

    static Object[][] testSignatureStatus() {
        return new Object[][]{
                {PERSONAL, null, "RETAIL", "esign"},
                {PERSONAL, null, "RETAIL", "wsign"},
                // Validate we can do Non-Personal customers
                {NON_PERSONAL, null, "RETAIL", "wsign"},
                // Validate Signature is overwritten
                {PERSONAL, new Signature().type("wsign"), "RETAIL", "esign"},
        };
    }

    @MethodSource
    @ParameterizedTest
    void testSignatureStatus(Customer customer, Signature original, String department, String type) {
        UUID id = UUID.randomUUID();
        Wire expected = new Wire();
        expected.setId(id);
        expected.setOriginator(map(customer));
        expected.department(department);
        expected.setSignature(original);
        expected.setPurpose("wire payment");
        expected.setType("domestic");
        expected.setExecution(new Execution().referenceNumber("UNIT-000001"));
        expected.setEffectiveDate(EFFECTIVE_DATE);
        expected.setStatus("pending_signature");
        expected.setAmount(BigDecimal.valueOf(1000));
        expected.setFeesOrCharges("100");
        wires.save(expected);
        PublishSpecification.given(signatures)
                .header("id", id.toString())
                .key(id.toString())
                .value(SignatureStatus.newBuilder().setStatus("Success").setModeOfSign(type).build())
                .send(kafka)
                .then(registry, factory)
                .waitAtMost(30, TimeUnit.SECONDS);

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> !Objects.equals(wires.findById(id).get().getSignature(), original));
        expected.setSignature(new Signature().type(type));
        expected.setTag(2);
        expected.addActivityItem(new Event().type("update").timestamp(TODAY));
        assertEquals(expected, wires.findById(id).get());
    }
}
