package model;

public class IpRange {
    long ipFrom;
    long ipTo;

    public IpRange(long ipFrom, long ipTo) {
        this.ipFrom = ipFrom;
        this.ipTo = ipTo;
    }

    public long getIpFrom() {
        return ipFrom;
    }

    public void setIpFrom(long ipFrom) {
        this.ipFrom = ipFrom;
    }

    public long getIpTo() {
        return ipTo;
    }

    public void setIpTo(long ipTo) {
        this.ipTo = ipTo;
    }

    public boolean isInRange(long ip) {
        return ip >= ipFrom && ip <= ipTo;
    }

    @Override
    public String toString() {
        return "IpRange{" +
                "ipFrom=" + ipFrom +
                ", ipTo=" + ipTo +
                '}';
    }
}
