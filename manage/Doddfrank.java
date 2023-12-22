package com.td.gwiro.wires.business;

import com.td.gwiro.wires.config.ReferenceData;
import com.td.gwiro.wires.config.WireConfig;
import com.td.gwiro.wires.repos.Wire;
import com.td.gwiro.wires.repos.WireRepository;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;

import org.apache.commons.text.StringEscapeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.List;

@Component
@Slf4j
@ConditionalOnProperty(value = "cron.doddfrank.enabled", havingValue = "true")
public class DoddFrankScheduler {

    @Autowired
    private WireConfig config;
    @Autowired

    private WireRepository wireRepository;

    @Autowired
    private PaymentExecutionService payment;

    @Autowired
    ReferenceData referenceData;

    @Autowired
    EffectiveDateFinder effectiveDateFinder;

    @Scheduled(cron = "#{effectiveDateFinder.getDoddFrankCronExpression()}", zone = "America/New_York")
    @SchedulerLock(name = "doddFrankCancellation")
    public void run() {
        log.info("Dodd Frank Scheduler processing ... time: {}", OffsetDateTime.now());
        /***
         * check if current job within configuration start/end time window
         <---* Since we now decide to run cron 24h/interval daily, remove time check *--->
         OffsetDateTime now = effectiveDateFinder.getCurrentDateTime();
         OffsetDateTime cronStartTime = referenceData.getDoddFrankTimes().getStart_time().atDate(now.toLocalDate());
         OffsetDateTime cronEndTime = referenceData.getDoddFrankTimes().getEnd_time().atDate(now.toLocalDate());
         if (cronStartTime.isBefore(now) && cronEndTime.isAfter(now)) {
         else {
         log.info("Dodd Frank Scheduler is out of configuration circle, " +
         "[ start time: {} , end time: {}, now: {}", cronStartTime, cronEndTime, now);
         }
         ***/

        OffsetDateTime now = effectiveDateFinder.getCurrentDateTime();
        //Get wires from MongoDB which has DFFlag : True and personal Interrnational
        List<Wire> wires = wireRepository.findByDoddFrankWindow(true);
        for (Wire wire : wires) {
            log.info("Processing Dodd Frank wire id: {}", wire.getId());
            try {
                OffsetDateTime doddFrankend = wire.getInternationalWire().getDoddFrank().getCancellationWindowEnd();
                if (doddFrankend.isBefore(now)) {
                    //then update DFFileds for wire -> DFFLag: false
                    log.debug("cancellation window expired, set flag to false, wire id: {} ,  wire status: {}", StringEscapeUtils.escapeJava(""+wire.getId()), StringEscapeUtils.escapeJava(wire.getStatus()));
                    wire.getInternationalWire().getDoddFrank().setInCancellationWindow(false);
                    //save wire to MongoDB
                    wireRepository.save(wire);
                    /*** Send payment request if state changed and is now in pending_transfer
                     We need to do that after the DB save to make sure the latest version of the
                     wire is available to listening systems  ***/
                    if (config.getStates().getPendingTransfer().equalsIgnoreCase(wire.getStatus())) {
                        log.info("wire is pending transfer, sending payment request");
                        payment.sendRequest(wire);
                    }
                }
            } catch (Exception e) {
                log.error("Error occurred during processing wire, {}", e.getMessage(), e);
            }

        }
    }

}

