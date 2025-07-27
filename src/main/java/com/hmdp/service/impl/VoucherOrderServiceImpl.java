package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
      private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct//当前类初始化完毕就执行
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHendles());
    }

      private class VoucherOrderHendles implements Runnable{
        String queueName = "stream.orders";
        @Override
        public void run() {
            while (true){
                try {
                    //1,获取消息队列中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2，判断消息获取是否成功
                    if(list == null || list.isEmpty()){
                        //2.1如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    //3,解析订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //4,如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    //5,ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    try {
                        handlePendingList();
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }

                }
            }
        }

          private void handlePendingList() throws InterruptedException {
              while (true){
                  try {
                      //1,获取PendingList中的订单信息
                      List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                              Consumer.from("g1", "c1"),
                              StreamReadOptions.empty().count(1),
                              StreamOffset.create(queueName, ReadOffset.from("0"))
                      );
                      //2，判断消息获取是否成功
                      if(list == null || list.isEmpty()){
                          //2.1如果获取失败，说明PendingList没有消息,结束循环
                          break;
                      }
                      //3,解析订单信息
                      MapRecord<String, Object, Object> record = list.get(0);
                      Map<Object, Object> value = record.getValue();
                      VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                      //4,如果获取成功，可以下单
                      handleVoucherOrder(voucherOrder);
                      //5,ACK确认
                      stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                  } catch (Exception e) {
                      log.error("处理PendingList异常",e);
                  }
              }
          }
      }
    /*//阻塞队列实现异步下单
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);
    private class VoucherOrderHendles implements Runnable{

        @Override
        public void run() {
            while (true){
                try {
                    //1,获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    //2,创建订单,进行处理
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                }
            }
        }
    }*/

    /**
     * 订单处理
     * @param voucherOrder
     */
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //1.获取用户
        Long userId = voucherOrder.getUserId();
        //2.创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //3.获取锁
        boolean isLock = lock.tryLock();
        //4.判断是否获取锁成功
        if(!isLock){
            //获取锁失败,返回错误或重试
            log.error("不允许重复下单");
            return;
        }
        try {
             proxy.createVoucherOrder(voucherOrder);
        } catch (Exception e){
            log.error("proxy.createVoucherOrder出现异常",e);
        }finally {
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //订单id
        long orderId = redisIdWorker.nextId("order");
        //1,执行lua脚本(判断购买资格并发送订单信息到消息队列）
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId)
        );
        //2,判断结果是否为0
        int r = result.intValue();
        if(r != 0){
            //2.1 结果不为0，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        //3,获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //4,返回订单id
        return Result.ok(orderId);
    }
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //1,执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        //2,判断结果是否为0
        int r = result.intValue();
        if(r != 0){
            //2.1 结果不为0，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        //2.2 结果为0，有购买资格，把下单信息保存到阻塞队列
        //保存阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        //订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //用户id
        userId = UserHolder.getUser().getId();
        voucherOrder.setUserId(userId);
        //代金券id
        voucherOrder.setVoucherId(voucherId);

        //2.3放入阻塞队列
        orderTasks.add(voucherOrder);
        //3,获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //4,返回订单id
        return Result.ok(orderId);
    }*/

   /* @Override
    public Result seckillVoucher(Long voucherId) {
        //1,查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2,判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            //尚未开始
            return Result.fail("秒杀尚未开始");
        }
        //3,判断秒杀是否结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            //尚未开始
            return Result.fail("秒杀已经结束");
        }
        //4,判断库存是否充足
        if (voucher.getStock() < 1) {
            //库存不足
            return Result.fail("库存不足");
        }

        //5，使用分布式锁处理Redis集群中出现的并发访问bug的问题
        Long userId = UserHolder.getUser().getId();
        //创建锁对象
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        //使用redisson提供的锁服务
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if(!isLock){
            //获取锁失败,返回错误或重试
            return Result.fail("不允许重复下单");
        }
        try {
            //获取代理对象（事务）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }

    }*/

    @Transactional
    public  void createVoucherOrder(VoucherOrder voucherOrder) {
        //5,一人一单实现(需要加悲观锁)
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        //使用用户id的值作为锁
            //5.1查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            //5.2判断是否存在
            if (count > 0) {
                //用户已经购买过，不允许下单
                log.error("用户已经购买过一次");
                return ;
            }

            //5,扣减库存(使用乐观锁解决线程安全问题)
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId).gt("stock", 0)
                    .update();
            if (!success) {
                log.error("库存不足");
                return ;
            }
            //创建订单
            save(voucherOrder);
    }
}
