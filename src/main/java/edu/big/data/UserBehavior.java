package edu.big.data;

import org.apache.hadoop.hbase.util.Bytes;

import java.time.LocalDateTime;


public class UserBehavior {
    public static final byte[] UID_FILED = Bytes.toBytes("uid");
    public static final byte[] AID_FILE = Bytes.toBytes("aid");
    public static final byte[] BEHAVIOR_FILED = Bytes.toBytes("type");

    private String uid;

    private String aid;

    private String behavior;

    private LocalDateTime behaviorTime;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getAid() {
        return aid;
    }

    public void setAid(String aid) {
        this.aid = aid;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public LocalDateTime getBehaviorTime() {
        return behaviorTime;
    }

    public void setBehaviorTime(LocalDateTime behaviorTime) {
        this.behaviorTime = behaviorTime;
    }
}
