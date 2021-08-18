package com.ververica.demo.backend.repositories;

import javax.persistence.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SqlRepositoryEvent {
  @Column(length=10000)
  public String content;

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Integer id;
}
