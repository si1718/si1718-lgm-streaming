package data.streaming.dto;

public class RatingDTO {
	String isbn1;
	String isbn2;
	Integer rating;
	String idRating;

	public RatingDTO(String isbn1, String isbn2, Integer rating, String idRating) {
		super();
		this.isbn1 = isbn1;
		this.isbn2 = isbn2;
		this.rating = rating;
		this.idRating = idRating;
	}

	public RatingDTO() {
		
	}


	public String getIsbn1() {
		return isbn1;
	}

	public String getIsbn2() {
		return isbn2;
	}

	public Integer getRating() {
		return rating;
	}
	
	public String getId() {
		return idRating;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((isbn1 == null) ? 0 : isbn1.hashCode());
		result = prime * result + ((isbn2 == null) ? 0 : isbn2.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RatingDTO other = (RatingDTO) obj;
		if (isbn1 == null) {
			if (other.isbn1 != null)
				return false;
		} else if (!isbn1.equals(other.isbn1))
			return false;
		if (isbn2 == null) {
			if (other.isbn2 != null)
				return false;
		} else if (!isbn2.equals(other.isbn2))
			return false;
		return true;
	}

	public String toString() {
		return "RatingDTO [isbn1=" + isbn1 + ", isbn2=" + isbn2 + ", rating=" + rating
				+ ", id=" + idRating + "]";
	}

}
