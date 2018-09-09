package ml.wonwoo.redismanager.web;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.reactive.result.view.Rendering;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


@Controller
public class RedisManagerController {

    private final ReactiveRedisOperations<Object, Object> template;

    public RedisManagerController(ReactiveRedisOperations<Object, Object> createRedisTemplate) {
        this.template = createRedisTemplate;
    }

    @GetMapping("/")
    public Rendering keys() {
        return Rendering.view("index")
                .modelAttribute("keys", this.template.keys("*"))
                .build();
    }

    @GetMapping("/redis/{key}")
    public Rendering hash(@PathVariable String key) {
        return Rendering.view("detail")
                .modelAttribute("hash", this.template.type(key)
                        .flatMapMany(dataType -> dataTypeOperation(dataType, key)))
                .modelAttribute("expire", this.template.getExpire(key))
                .build();
    }

    private Flux<?> dataTypeOperation(DataType dataType, String key) {
        if (dataType.equals(DataType.HASH)) {
            return this.template.opsForHash().entries(key);
        } else if (dataType.equals(DataType.SET)) {
            return this.template.opsForSet().members(key);
        } else if (dataType.equals(DataType.LIST)) {
            return this.template.opsForList().range(key, 0, -1);
        } else if (dataType.equals(DataType.STRING)) {
            return this.template.opsForValue().get(key).flux();
        } else if (dataType.equals(DataType.ZSET)) {
            return this.template.opsForZSet().range(key, Range.unbounded());
        }
        return Flux.error(new UnsupportedOperationException(dataType.code() + " data type not supported"));
    }
}