package io.debezium.examples.ticketmsa.invoice;

import javax.enterprise.context.ApplicationScoped;

import org.aerogear.kafka.cdi.annotation.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class InvoiceService {

    private static final Logger LOGGER = LoggerFactory.getLogger(InvoiceService.class);

    @Consumer(topics = "#{ORDER_TOPIC_NAME}", groupId = "InvoiceService")
    public void orderArrived(final String order) {
        LOGGER.info("Order event '{}' arrived", order);
    }
}
