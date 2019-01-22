package com.mywork.kafkaproject.producer;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.mywork.kafkaproject.constants.IKafkaConstants;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
@Api(value = "Kafka Producer API") // @Api on Controller class to describe our API.
public class ProducerController {
  private static final Logger logger = LoggerFactory.getLogger(ProducerController.class);

  @ApiOperation(value = "Run Producer to produce messages", response = Object.class) // to describe each
                                                                                     // method
  // to describe other responses other than 200
  @ApiResponses(value = { @ApiResponse(code = 200, message = "Successfully ran producer"),
      @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
      @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
      @ApiResponse(code = 404, message = "The resource you were trying to reach is not found") })
  @RequestMapping(value = "/run/producer", method = RequestMethod.GET, produces = "application/json")
  public ResponseEntity<Object> runProducer() {
    Producer<Long, String> producer = ProducerCreator.createProducer();

    for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
      final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME,
          "This is record " + index);
      try {
        RecordMetadata metadata = producer.send(record).get();
        logger.info("Record sent with key " + index + " to partition " + metadata.partition() + " with offset "
            + metadata.offset());

      } catch (ExecutionException e) {

        logger.error("Error in sending record: " + e);
      } catch (InterruptedException e) {
        logger.error("Error in sending record: " + e);
      }
    }
    return new ResponseEntity<>("producer RAN successfully", HttpStatus.OK);
  }
}
