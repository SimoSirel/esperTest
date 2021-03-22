public class TemperatureObs {
    private final int sid;
    private final double tv;

    public TemperatureObs(int sid, double tv) {
        this.sid = sid;
        this.tv = tv;
    }

    public int getSid() {
        return sid;
    }

    public double getTv() {
        return tv;
    }
}
