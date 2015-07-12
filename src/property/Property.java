package property;

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class Property {
	
	    @SerializedName("time_stamp")
		public String time_stamp;
		
	    @SerializedName("user_id")
		public String user_id;
		
		@SerializedName("location")
		public List<Location> location;
}
