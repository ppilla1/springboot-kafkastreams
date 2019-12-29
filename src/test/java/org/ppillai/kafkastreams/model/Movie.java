package org.ppillai.kafkastreams.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Setter
@Getter
@ToString
public class Movie {
    private String title;
    private Integer year;
    private List<String> cast;
    private List<String> genres;
}
