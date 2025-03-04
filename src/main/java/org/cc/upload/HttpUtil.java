package org.cc.upload;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;


public class HttpUtil{

	public static JSONObject doGet(String httpurl) {
        HttpURLConnection connection = null;
        InputStream is = null;
        BufferedReader br = null;
        JSONObject result = new JSONObject();
        try {
            URL url = new URL(httpurl);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(30000);
            connection.setReadTimeout(60000);
            connection.connect();
            if (connection.getResponseCode() == 200) 
            {	
            	Map<String,List<String>> respone_headers=connection.getHeaderFields();
            	for (String key : respone_headers.keySet()) {
            		if(key!=null &&key.equals("Set-Cookie")) {
            			List<String> string = respone_headers.get(key);
            			StringBuilder builder = new StringBuilder();
            			 for (String str : string) {
            				 builder.append(str).toString();
            			 }
            			 String cookie=builder.toString();
            			 result.put("cookie", cookie);
            		}
            	}
            	
                is = connection.getInputStream();
                br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                StringBuffer sbf = new StringBuffer();
                String temp = null;
                while ((temp = br.readLine()) != null) {
                    sbf.append(temp);
                    sbf.append("\r\n");
                }

                result.put("data", sbf.toString());
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != br) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            connection.disconnect();
            connection = null;
        }

        return result;
    }
	
    public static String doPost(String httpUrl, String param) 
    {
        HttpURLConnection connection = null;
        InputStream is = null;
        OutputStream os = null;
        BufferedReader br = null;
        String result = null;
        try {
            URL url = new URL(httpUrl);
            // 通过远程url连接对象打开连接
            connection = (HttpURLConnection) url.openConnection();
            // 设置连接请求方式
            connection.setRequestMethod("POST");
            connection.setConnectTimeout(60000);
            connection.setReadTimeout(30000);

            // 默认值为：false，当向远程服务器传送数据/写数据时，需要设置为true
            connection.setDoOutput(true);
            // 默认值为：true，当前向远程服务读取数据时，设置为true，该参数可有可无
            connection.setDoInput(true);
            
            // 设置传入参数的格式:请求参数应该是 name1=value1&name2=value2 的形式。
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            // 设置鉴权信息：Authorization: Bearer da3efcbf-0845-4fe3-8aba-ee040be542c0
            //connection.setRequestProperty("Authorization", "Bearer da3efcbf-0845-4fe3-8aba-ee040be542c0");
            // 通过连接对象获取一个输出流
            os = connection.getOutputStream();
            // 通过输出流对象将参数写出去/传输出去,它是通过字节数组写出的
            os.write(param.getBytes());
            os.flush();
            os.close();
            // 通过连接对象获取一个输入流，向远程读取
            if (connection.getResponseCode() == 200) {

                is = connection.getInputStream();
                // 对输入流对象进行包装:charset根据工作项目组的要求来设置
                br = new BufferedReader(new InputStreamReader(is, "UTF-8"));

                StringBuffer sbf = new StringBuffer();
                String temp = null;
                // 循环遍历一行一行读取数据
                while ((temp = br.readLine()) != null) {
                    sbf.append(temp);
                    sbf.append("\r\n");
                }
                result = sbf.toString();
                sbf.setLength(0);
                sbf = null;
                
//                ServerStorage.addLog(result);
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            if (null != br) {
                try {
                    br.close();
                    br = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != os) {
                try {
                    os.close();
                    os = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != is) {
                try {
                    is.close();
                    is = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            // 断开与远程地址url的连接
            connection.disconnect();
            connection = null;
        }
        return result;
    }
    
    public static String doPostJson(String httpUrl, String param) 
    {
        HttpURLConnection connection = null;
        InputStream is = null;
        OutputStream os = null;
        BufferedReader br = null;
        String result = null;
        try {
            URL url = new URL(httpUrl);
            // 通过远程url连接对象打开连接
            connection = (HttpURLConnection) url.openConnection();
            // 设置连接请求方式
            connection.setRequestMethod("POST");
            connection.setConnectTimeout(60000);
            connection.setReadTimeout(30000);

            // 默认值为：false，当向远程服务器传送数据/写数据时，需要设置为true
            connection.setDoOutput(true);
            // 默认值为：true，当前向远程服务读取数据时，设置为true，该参数可有可无
            connection.setDoInput(true);
            
            
            // 设置传入参数的格式:请求参数应该是 name1=value1&name2=value2 的形式。
            connection.setRequestProperty("Content-Type", "application/json");

            // 设置鉴权信息：Authorization: Bearer da3efcbf-0845-4fe3-8aba-ee040be542c0
            //connection.setRequestProperty("Authorization", "Bearer da3efcbf-0845-4fe3-8aba-ee040be542c0");
            // 通过连接对象获取一个输出流
            os = connection.getOutputStream();
            // 通过输出流对象将参数写出去/传输出去,它是通过字节数组写出的
            os.write(param.getBytes());
            os.flush();
            os.close();
            // 通过连接对象获取一个输入流，向远程读取
            if (connection.getResponseCode() == 200) {

                is = connection.getInputStream();
                // 对输入流对象进行包装:charset根据工作项目组的要求来设置
                br = new BufferedReader(new InputStreamReader(is, "UTF-8"));

                StringBuffer sbf = new StringBuffer();
                String temp = null;
                // 循环遍历一行一行读取数据
                while ((temp = br.readLine()) != null) {
                    sbf.append(temp);
                    sbf.append("\r\n");
                }
                result = sbf.toString();
                sbf.setLength(0);
                sbf = null;
                
//                ServerStorage.addLog(result);
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            if (null != br) {
                try {
                    br.close();
                    br = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != os) {
                try {
                    os.close();
                    os = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != is) {
                try {
                    is.close();
                    is = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            // 断开与远程地址url的连接
            connection.disconnect();
            connection = null;
        }
        return result;
    }
    
    
    public static void PostAsync(String httpUrl, String param)
    {
        CloseableHttpAsyncClient client = HttpAsyncClients.createDefault();
        client.start();
        final CountDownLatch latch = new CountDownLatch(1);
        final HttpPost post = new HttpPost(httpUrl);
        
        post.addHeader("Content-type", "application/x-www-form-urlencoded");
        StringEntity entity = null;
        try {
            entity = new StringEntity(param);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        post.setEntity(entity);
        
        client.execute(post,new FutureCallback<HttpResponse>(){
        	//执行异步操作  请求完成后
            public void completed(final HttpResponse response) {
                latch.countDown();
                //响应内容
//                try {
////					ServerStorage.addLog(response.getStatusLine().getStatusCode()
////					        + "  " + EntityUtils.toString(response.getEntity(), "UTF-8"));
//				} catch (ParseException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				} catch (IOException e) 
//                {
//					e.printStackTrace();
//                }
            }

			@Override
			public void cancelled() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void failed(Exception arg0) {
				// TODO Auto-generated method stub
				
			}
        });
    }
    
    
    public static void SendDataToClient(JSONObject obj,String url,boolean debug)
    {
    	String param_s = GetParam(obj);
    	
//    	if(debug)
//    	{
//    		ServerStorage.Log(param_s);
//    		tool.appendInfoToFile("params", param_s);
//    	}
    	
    	HttpUtil.PostAsync(url, param_s);
    }
    
    public static String GetParam(JSONObject obj) {
		StringBuffer sb = new StringBuffer();
		
		for(String key : obj.keySet())
		{
			sb.append("&");
			sb.append(key).append("=").append(obj.get(key));
		}
		if(sb.length() > 0)
			sb.deleteCharAt(0);
			
		return sb.toString();
	}
}