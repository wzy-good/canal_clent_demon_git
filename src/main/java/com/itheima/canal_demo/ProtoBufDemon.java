package com.itheima.canal_demo;

import com.google.protobuf.InvalidProtocolBufferException;
import com.itheima.protobuf.DemoModel;

public class ProtoBufDemon {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        DemoModel.User.Builder builder = DemoModel.User.newBuilder();
        //赋值
        builder.setId(1);
        builder.setName("张三");
        builder.setSex("男");

       /* DemoModel.User userBuild = builder.build();
        System.out.println(userBuild.getSex());
*/

       //将一个对象序列化成二进制字节码(这个字节码数据就是要写道kafka的数据)
        byte[] bytes = builder.build().toByteArray();
        for(byte b: bytes){
            System.out.println(b);
        }

        //从kafka中消费出来的数据是二进制字节码
        //将二进制字节码反序列化成对象
        DemoModel.User user = DemoModel.User.parseFrom(bytes);
        System.out.println(user.getSex()+"--"+user.getName());
    }
}
