package org.ppillai.kafkastreams.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Setter
@Getter
@ToString
public class MovieGenere {
    private String title;
    private List<String> genres;
}
