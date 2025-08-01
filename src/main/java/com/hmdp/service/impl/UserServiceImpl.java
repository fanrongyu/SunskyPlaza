package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import static com.hmdp.utils.RedisConstants.*;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;


    @Override
    public Result sedCode(String phone, HttpSession session) {
        //1. 校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            //2.如果不符合，返回错误信息
            return Result.fail("手机号格式错误");
        }

        //3. 符合，生成验证码
        String code = RandomUtil.randomNumbers(6);
        //4. 保存验证码到session
        //session.setAttribute("code",code);
        //4,保存验证码到redis //set ley value ex 120
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY+phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);

        //5. 发送验证码(不做)
        log.debug("发送短信验证码成功，验证码:{}",code);
        //返回ok
        return Result.ok();
    }

    /**
     * 实现签到功能
     * @return
     */
    @Override
    public Result sign() {
        //1,获取当前用户
        Long userId = UserHolder.getUser().getId();
        //2,获取日期
        LocalDateTime now = LocalDateTime.now();
        //3,拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //4,获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //5,写入redis SETBIT key offset
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth-1,true);
        return Result.ok();
    }

    /**
     * 实现查询本月连续签到天数
     * @return
     */
    @Override
    public Result signCount() {

        //1,获取当前用户
        Long userId = UserHolder.getUser().getId();
        //2,获取日期
        LocalDateTime now = LocalDateTime.now();
        //3,拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //4,获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //5,获取本月截止今天为止所有签到记录，返回一个十进制数字
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0)
        );
        if(result == null || result.isEmpty()){
            //没有任何签到结果
            return Result.ok(0);
        }
        Long num = result.get(0);
        if(num == null || num == 0){
            //没有任何签到结果
            return Result.ok(0);
        }
        //6,循环遍历
        int count = 0;
        while (true){
            //6.1让这个数字与1做与匀速那，得到数字的最后一个bit位
            //判断bit位知否为0
            if((num & 1) == 0){
                //如果为零，说明未签到，结束
                break;
            }else {
                //如果不为0，说明已签到，计数器加一
                count++;
            }
            //数字右移一位，抛弃最后一个bit位，继续下一个bit位
            num >>>= 1;//num = num >> 1;
        }
        return Result.ok(count);
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1. 校验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误");
        }
        //2. 校验验证码,从redis获取
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (cacheCode == null || !cacheCode.equals(code)){
            //3. 不一致，报错
            return Result.fail("验证码错误");
        }

        //4.一致，根据手机号查询用户
        User user = query().eq("phone", phone).one();

        //5. 判断用户是否存在
        if (user == null){
            //6. 不存在，创建新用户
            user = createUserWithPhone(phone);
        }

        //7.保存用户信息到reids
        //7.1 随机生成tocken
        String tocken = UUID.randomUUID().toString(true);
        //7.2 将User对象转为HashMap存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        //map中默认会将userDTO中的元素按照原本格式转化成MAP，
        // 但是stringRedisTemplate.opsForHash()中不允许出现long类型数据，因此需要提前进行转换
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create()
                .setIgnoreNullValue(true)
                .setFieldValueEditor((fieldName,fieldValue)->fieldValue.toString()));
        //7.3 存储数据
        String tockenKey = LOGIN_USER_KEY + tocken;

        stringRedisTemplate.opsForHash().putAll(tockenKey,userMap);
        //7.4 设置tocken有效期
        stringRedisTemplate.expire(tockenKey,LOGIN_USER_TTL,TimeUnit.MINUTES);

        //8,返回tocken
        return Result.ok(tocken);
    }



    private User createUserWithPhone(String phone) {
        // 1.创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        // 2.保存用户
        save(user);
        return user;
    }
}
