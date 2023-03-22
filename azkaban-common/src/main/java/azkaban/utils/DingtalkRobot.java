package azkaban.utils;

import azkaban.Constants;
import azkaban.alert.Alerter;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.sla.SlaOption;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.inject.Inject;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DingtalkRobot implements Alerter {

    private static final String HTTPS = "https";
    private static final String HTTP = "http";
    private static final String webhook = "https://oapi.dingtalk.com/robot/send?access_token=";
    private final String token;
    private final String scheme;
    private final String clientHostname;
    private final String clientPortNumber;
    private final String azkabanName;
    private final String signature;

    @Inject
    public DingtalkRobot(final Props props) {
        this.token = props.getString("dingtalk.token", null);
        this.signature = props.getString("dingtalk.signature", null);
        this.azkabanName = props.getString("azkaban.name", "azkaban");

        this.clientHostname = props.getString(Constants.ConfigurationKeys.AZKABAN_WEBSERVER_EXTERNAL_HOSTNAME,
                props.getString("jetty.hostname", "localhost"));

        if (props.getBoolean("jetty.use.ssl", true)) {
            this.scheme = HTTPS;
            this.clientPortNumber = Integer.toString(props
                    .getInt(Constants.ConfigurationKeys.AZKABAN_WEBSERVER_EXTERNAL_SSL_PORT,
                            props.getInt("jetty.ssl.port",
                                    Constants.DEFAULT_SSL_PORT_NUMBER)));
        } else {
            this.scheme = HTTP;
            this.clientPortNumber = Integer.toString(
                    props.getInt(Constants.ConfigurationKeys.AZKABAN_WEBSERVER_EXTERNAL_PORT, props.getInt("jetty.port",
                            Constants.DEFAULT_PORT_NUMBER)));
        }
    }

    private static List<String> findFailedJobs(final ExecutableFlow flow) {
        final ArrayList<String> failedJobs = new ArrayList<>();
        for (final ExecutableNode node : flow.getExecutableNodes()) {
            if (node.getStatus() == Status.FAILED) {
                failedJobs.add(node.getId());
            }
        }
        return failedJobs;
    }

    private void send(String body) throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeyException {
        Long timestamp = System.currentTimeMillis();
        String stringToSign = timestamp + "\n" + signature;
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(signature.getBytes("UTF-8"), "HmacSHA256"));
        byte[] signData = mac.doFinal(stringToSign.getBytes("UTF-8"));
        String sign = URLEncoder.encode(new String(Base64.encodeBase64(signData), StandardCharsets.UTF_8),"UTF-8");
        PostMethod method = new PostMethod(webhook + token + "&timestamp=" + timestamp + "&sign=" + sign);
        try {
            HttpClient client = new HttpClient();
            method.setRequestHeader("Content-Type", "application/json");
            method.setRequestEntity(new StringRequestEntity(body, "application/json", "UTF-8"));
            int rspCode = client.executeMethod(method);
            System.out.printf(String.valueOf(rspCode));
            System.out.printf(method.getResponseBodyAsString());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
           method.releaseConnection();
        }
    }

    @Override
    public void alertOnSuccess(ExecutableFlow exflow) throws Exception {

    }

    @Override
    public void alertOnError(ExecutableFlow flow, String... extraReasons) throws Exception {
        if (Objects.isNull(this.token)) {
            return;
        }
        DingtalkMessage message = new DingtalkMessage();
        message.setTitle("Flow " + flow.getFlowId() + " has encountered a failure on " + azkabanName);
        String header = "# Execution " + flow.getExecutionId() + " of flow " + flow.getFlowId()
                + " of project " + flow.getProjectName() + " has encountered a failure on " + azkabanName + " \n";
        String startTime = "- Start Time: " + TimeUtils.formatDateTimeZone(flow.getStartTime()) + " \n";
        String endTime = "- End Time: " + TimeUtils.formatDateTimeZone(flow.getEndTime()) + " \n";
        String duration = "- Duration: " + TimeUtils.formatDuration(flow.getStartTime(), flow.getEndTime()) + "\n";
        String status = "- Status: " + flow.getStatus() + " \n";
        final String executionUrl = scheme + "://" + clientHostname + ":" + clientPortNumber + "/"
                + "executor?" + "execid=" + flow.getExecutionId();
        String executionLink = "- [Execution Link](" + executionUrl + ") \n";
        String reasonSection = "## Reason \n";
        final List<String> failedJobs = findFailedJobs(flow);
        StringBuilder reasons = new StringBuilder();
        for (final String jobId: failedJobs) {
            reasons.append("- [Failed job '").append(jobId).append("' Link](").append(executionUrl).append("&job=").append(jobId).append(") \n");
        }
        for (String extraReason : extraReasons) {
            reasons.append("- ").append(extraReason).append("\n");
        }
        message.setText(header, startTime, endTime, duration, status, executionLink, reasonSection, reasons.toString());
        send(message.toString());
    }

    @Override
    public void alertOnFirstError(ExecutableFlow exflow) throws Exception {

    }

    @Override
    public void alertOnSla(SlaOption slaOption, String slaMessage) throws Exception {

    }

    @Override
    public void alertOnFailedUpdate(Executor executor, List<ExecutableFlow> executions, ExecutorManagerException e) {

    }

    @Override
    public void alertOnFailedExecutorHealthCheck(Executor executor, List<ExecutableFlow> executions, ExecutorManagerException e, List<String> alertEmails) {

    }
}
