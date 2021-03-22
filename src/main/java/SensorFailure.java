public class SensorFailure {
    private String type;
    private int sid;
    private int code;

    public SensorFailure(String type, int sid, int code){
        this.type = type;
        this.sid = sid;
        this.code = code;
    }

    public String getType() {
        return type;
    }
    public int getSid() {
        return sid;
    }
    public int getCode() {
        return code;
    }


}
