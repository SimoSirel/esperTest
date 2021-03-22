public class HumidityObs extends TemperatureObs{

    private int hv;

    public HumidityObs(int sid, double tv, int hv) {
        super(sid, tv);
        this.hv = hv;
    }

    public int getHv() {
        return hv;
    }
}
