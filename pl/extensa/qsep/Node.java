package pl.extensa.qsep;

/**
 * Lightweight node used as data container
 * @author berni
 *
 */
public class Node
{
    public String sku;
    public int quantity;
    public double price;
	public int updated; // number of updated rows
	public int product_id;
	
	@Override
	public String toString() {
		return "SKU: " + sku + " QUANT: " + quantity + " PRICE: "  + price + " UPD? " + updated + " product_id " + product_id;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Node)
			return sku == ((Node)obj).sku && quantity == ((Node)obj).quantity && price == ((Node)obj).price;
		return false;
	}
}
