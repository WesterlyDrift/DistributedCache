package com.kvstore.raft.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class Command implements Serializable {
    private static final long serialVersionUID = 1L;

}
