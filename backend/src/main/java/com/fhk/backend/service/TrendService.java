package com.fhk.backend.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fhk.backend.api.TrendItem;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

// redis zset - 실시간 랭킹 읽고 / hash로 메타 병합
// 결과값 http api로 반환, or sse 이벤트로 스트림
// sse = server-sents events. 브라우저로 실시간 데이터 스트림 하는 방식

@Service
@RequiredArgsConstructor
public class TrendService {
    private final ReactiveStringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // (1) 리액티브 레디스 클라이언트
    // (2) 리스트를 json 문자열로 직렬화

    private static final String TITLE = "title";
    private static final String SENTIMENT = "sentiment";
    private static final String TOPIC_ID = "topic_id";
    private static final String DEFAULT_WINDOW = "5m";
    private static final int DEFAULT_SIZE = 20;
    private static final int MAX_SIZE = 100;
    private static final Duration STREAM_TICK = Duration.ofSeconds(5);
    // 몇 초에 한 번 푸시할 건지

    private String windowOrDefault(String window) {
        return (window == null || window.isBlank()) ? DEFAULT_WINDOW : window;
    }

    private String globalRankKey(String window) {
        return "trend:zset:global:%s".formatted(windowOrDefault(window));
    }

    private String itemMetaKey(String id) {
        return "trend:hash:item:%s".formatted(id);
    }

    private int clampSize(int size) {
        if (size <= 0) return DEFAULT_SIZE;
        return Math.min(size, MAX_SIZE);
    }

    /**
     * 상위 N 트렌드 읽기: ZSET(점수) + HASH(메타) → TrendItem 리스트
     */
    public Mono<List<TrendItem>> getTopTrends(String window, int requestedSize) {
        final int size = clampSize(requestedSize);
        final String zsetKey = globalRankKey(window);

        return redisTemplate.opsForZSet()
                .reverseRangeWithScores(zsetKey, Range.closed(0L, (long)size - 1))
                .flatMap((ZSetOperations.TypedTuple<String> t) -> {
                    final String itemId = t.getValue();
                    final double score = t.getScore() == null ? 0.0 : t.getScore();

                    return redisTemplate.opsForHash()
                            .multiGet(itemMetaKey(itemId), List.of(TITLE, SENTIMENT, TOPIC_ID))
                            .map(vals -> new TrendItem(
                                    itemId,
                                    (String) vals.get(0), // title
                                    score,
                                    (String) vals.get(1), // sentiment
                                    (String) vals.get(2)  // topic_id
                            ));
                })
                .collectList();
    }

    /**
     * n초마다 최신 Top-N을 SSE로 푸시
     */
    public Flux<ServerSentEvent<String>> streamTopTrends(String window, int requestedSize) {
        final int size = clampSize(requestedSize);

        return Flux.interval(STREAM_TICK)
                .flatMap(tick -> getTopTrends(window, size))
                .map(list -> {
                    try {
                        return objectMapper.writeValueAsString(list);
                    } catch (Exception e) {
                        return "[]";
                    }
                })
                .map(json -> ServerSentEvent.<String>builder(json)
                        .event("rankUpdate")
                        .build()
                );
    }
}