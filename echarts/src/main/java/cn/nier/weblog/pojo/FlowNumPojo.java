package cn.nier.weblog.pojo;

public class FlowNumPojo {
    private Integer id;
    private String dateStr;
    private String pVNum;
    private String nVNum;
    private String iPNum;
    private String newUvNum;
    private String visitNum;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getDateStr() {
        return dateStr;
    }

    public void setDateStr(String dateStr) {
        this.dateStr = dateStr;
    }

    public String getpVNum() {
        return pVNum;
    }

    public void setpVNum(String pVNum) {
        this.pVNum = pVNum;
    }

    public String getnVNum() {
        return nVNum;
    }

    public void setnVNum(String nVNum) {
        this.nVNum = nVNum;
    }

    public String getiPNum() {
        return iPNum;
    }

    public void setiPNum(String iPNum) {
        this.iPNum = iPNum;
    }

    public String getNewUvNum() {
        return newUvNum;
    }

    public void setNewUvNum(String newUvNum) {
        this.newUvNum = newUvNum;
    }

    public String getVisitNum() {
        return visitNum;
    }

    public void setVisitNum(String visitNum) {
        this.visitNum = visitNum;
    }

    @Override
    public String toString() {
        return "FlowNumPojo{" +
                "id=" + id +
                ", dateStr='" + dateStr + '\'' +
                ", pVNum='" + pVNum + '\'' +
                ", nVNum='" + nVNum + '\'' +
                ", iPNum='" + iPNum + '\'' +
                ", newUvNum='" + newUvNum + '\'' +
                ", visitNum='" + visitNum + '\'' +
                '}';
    }
}
