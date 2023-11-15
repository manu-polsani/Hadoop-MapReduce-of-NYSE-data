import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class StockWritable implements Writable {
    
    private String date;
    private double open;
    private double high;
    private double low;
    private double close;
    private double adjClose;
    private long volume;

    public StockWritable() {}
    
    public StockWritable(StockWritable stock) {
        this.date = stock.date;
        this.open = stock.open;
        this.high = stock.high;
        this.low = stock.low;
        this.close = stock.close;
        this.adjClose = stock.adjClose;
        this.volume = stock.volume;
    }
    
    public void setDate(String date) {
        this.date = date;
    }
    
    public void setOpen(double open) {
        this.open = open;
    }
    
    public void setHigh(double high) {
        this.high = high;
    }
    
    public void setLow(double low) {
        this.low = low;
    }
    
    public void setClose(double close) {
        this.close = close;
    }
    
    public void setAdjClose(double adjClose) {
        this.adjClose = adjClose;
    }
    
    public void setVolume(long volume) {
        this.volume = volume;
    }
    
    public String getDate() {
        return this.date;
    }
    
    public double getOpen() {
        return this.open;
    }
    
    public double getHigh() {
        return this.high;
    }
    
    public double getLow() {
        return this.low;
    }
    
    public double getClose() {
        return this.close;
    }
    
    public double getAdjClose() {
        return this.adjClose;
    }
    
    public long getVolume() {
        return this.volume;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.date = in.readUTF();
        this.open = in.readDouble();
        this.high = in.readDouble();
        this.low = in.readDouble();
        this.close = in.readDouble();
        this.adjClose = in.readDouble();
        this.volume = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.date);
        out.writeDouble(this.open);
        out.writeDouble(this.high);
        out.writeDouble(this.low);
        out.writeDouble(this.close);
        out.writeDouble(this.adjClose);
        out.writeLong(this.volume);
    }

    @Override
    public String toString() {
        return "StockWritable{" +
                "date='" + date + '\'' +
                ", open=" + open +
                ", high=" + high +
                ", low=" + low +
                ", close=" + close +
                ", adjClose=" + adjClose +
                ", volume=" + volume +
                '}';
    }
}
