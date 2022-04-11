package me.changwoo.producer.controller;

import com.google.gson.Gson;
import me.changwoo.producer.configuration.ApiUrl;
import me.changwoo.producer.dto.UserEventDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.Date;


@RestController
@CrossOrigin(origins = "*", allowedHeaders = "*")
public class ProduceController {
    private final Logger logger = LoggerFactory.getLogger(ProduceController.class);
    private final KafkaTemplate<String, String> kafkaTemplate;

    // @Autowired : 단일 생성자의 경우, 굳이 필요가 없다.
    public ProduceController(KafkaTemplate<String, String> kafkaTemplate) { // constructor injection
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping(value = ApiUrl.SELECT_COLOR)
    public void selectColor(@RequestHeader(value = "user-agent") String userAgentName,
                            @RequestHeader(value = "color") String colorName,
                            @RequestHeader(value = "user") String userName) {

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
        Date now = new Date();
        Gson gson = new Gson();
        UserEventDto userEventDto = new UserEventDto(sdfDate.format(now), userAgentName, colorName, userName);

        String jsonColorLog = gson.toJson(userEventDto);
        kafkaTemplate.send("select-color", jsonColorLog).addCallback(
                new ListenableFutureCallback<SendResult<String, String>>() { // 정상적으로 전송되었는지 확인하기 위함
                    @Override
                    public void onFailure(Throwable ex) {
                        logger.error(ex.getMessage());
                    }

                    @Override
                    public void onSuccess(SendResult<String, String> result) {
                        logger.info(result.toString());
                    }
                }
        );


    }

}
