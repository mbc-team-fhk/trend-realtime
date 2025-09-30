package com.fhk.backend.controller;

import com.fhk.backend.api.TrendItem;
import com.fhk.backend.service.TrendService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/api/trends")
@RequiredArgsConstructor
public class TrendController {

    private final TrendService trendService;

    /**
     * 전역 Top-N 조회 (단건 응답)
     */
    @GetMapping("/global")
    public Mono<List<TrendItem>> getGlobalTrends(
            @RequestParam(defaultValue = "5m") String window,
            @RequestParam(defaultValue = "20") int size
    ) {
        return trendService.getTopTrends(window, size);
    }

    /**
     * 전역 Top-N 실시간 스트림 (20초마다 SSE 이벤트)
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> streamGlobalTrends(
            @RequestParam(defaultValue = "5m") String window,
            @RequestParam(defaultValue = "20") int size
    ) {
        return trendService.streamTopTrends(window, size);
    }
}