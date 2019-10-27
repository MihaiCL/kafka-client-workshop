package ro.ghita.kafkaapplication.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ProductWithEan {

    private String name;
    private Integer code;
    private String description;
    private String ean;

}
