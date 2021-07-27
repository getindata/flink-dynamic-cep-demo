package com.ververica.demo.backend.repositories;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SqlRepositoryEvent {
  public String content;

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Integer id;
}
