package org.cc;

import com.google.gson.Gson;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class CommandSender {

    /**
     * 发送 JSON 格式命令到指定的 IP 和端口。
     *
     * @param ip 目标设备的 IP 地址
     * @param port 目标设备的端口号
     * @param addr 设备地址
     * @param cmd 命令类型（如 "set"）
     * @param y DO 输出状态列表（1 表示闭合，0 表示断开）
     * @param count DI 脉冲计数值列表
     * @param delay DO 继电器延时断开时间列表（单位：毫秒）
     */
    public static void sendCommand(String ip, int port, int addr, String cmd, int[] y, int[] count, int[] delay) {
        try (Socket socket = new Socket(ip, port)) {
            System.out.println("Connected to " + ip + ":" + port);

            // 构建 JSON 数据包
            Command command = new Command(addr, cmd, y, count, delay);

            // 将命令转换为 JSON 格式
            Gson gson = new Gson();
            String jsonCommand = gson.toJson(command);
            System.out.println("Sending command: " + jsonCommand);

            // 发送数据
            OutputStream out = socket.getOutputStream();
            out.write(jsonCommand.getBytes("UTF-8"));
            out.flush();

            // 可选：接收响应
            // InputStream in = socket.getInputStream();
            // byte[] response = new byte[1024];
            // int readBytes = in.read(response);
            // if (readBytes != -1) {
            //     String responseStr = new String(response, 0, readBytes, "UTF-8");
            //     System.out.println("Received response: " + responseStr);
            // }
        } catch (IOException e) {
            System.err.println("An error occurred: " + e.getMessage());
        }
    }

    // JSON Command class
    static class Command {
        int addr;
        String cmd;
        int[] y;
        int[] count;
        int[] delay;

        public Command(int addr, String cmd, int[] y, int[] count, int[] delay) {
            this.addr = addr;
            this.cmd = cmd;
            this.y = y;
            this.count = count;
            this.delay = delay;
        }
    }

}