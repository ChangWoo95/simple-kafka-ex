package me.changwoo.producer.dto;

public class UserEventDto {

    private String timestamp; // API 호출받은 시점에 메세지 값에 넣는 역할
    private String userAgent; // API 호출받을 때 받을 수 있는 사용자 브라우저 종류
    private String colorName;
    private String userName;

    public UserEventDto(String timestamp, String userAgent, String colorName, String userName) {
        this.timestamp = timestamp;
        this.userAgent = userAgent;
        this.colorName = colorName;
        this.userName = userName;
    }

}
