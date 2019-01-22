package com.mywork.kafkaproject.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.mywork.kafkaproject.constants.IKafkaConstants;
import com.mywork.kafkaproject.producer.ProducerController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController

@Api(value = "Kafka Consumer API") // @Api on Controller class to describe our API.
public class ConsumerController {
  private static final Logger logger = LoggerFactory.getLogger(ProducerController.class);

  @ApiOperation(value = "Run consumer API to consume messages", response = Object.class) // to describe each
                                                                                         // method
  @ApiResponses(value = { @ApiResponse(code = 200, message = "Successfully ran consumer"),
      @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
      @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
      @ApiResponse(code = 404, message = "The resource you were trying to reach is not found") })
  @RequestMapping(value = "/run/consumer", method = RequestMethod.GET, produces = "application/json")
  public ResponseEntity<Object> runConsumer() {
    Consumer<Long, String> consumer = ConsumerCreator.createConsumer();

    int noMessageToFetch = 0;

    while (true) {
      final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
      if (consumerRecords.count() == 0) {
        noMessageToFetch++;
        if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
          break;
        else
          continue;
      }

      consumerRecords.forEach(record -> {
        logger.info("Record Key " + record.key());
        logger.info("Record value " + record.value());
        logger.info("Record partition " + record.partition());
        logger.info("Record offset " + record.offset());
      });
      consumer.commitAsync();
    }
    consumer.close();
    return new ResponseEntity<>("consumer RAN successfully", HttpStatus.OK);
  }
}
